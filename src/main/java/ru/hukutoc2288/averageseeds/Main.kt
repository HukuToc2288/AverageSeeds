package ru.hukutoc2288.averageseeds

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jsonMapper
import org.springframework.boot.SpringApplication
import retrofit2.Call
import retrofit2.HttpException
import ru.hukutoc2288.averageseeds.api.keeper.keeperRetrofit
import ru.hukutoc2288.averageseeds.api.seeds.SeedsRetrofit
import ru.hukutoc2288.averageseeds.entities.db.SeedsInsertItem
import ru.hukutoc2288.averageseeds.entities.db.SeedsSyncItem
import ru.hukutoc2288.averageseeds.utils.SeedsProperties
import ru.hukutoc2288.averageseeds.utils.SeedsRepository
import ru.hukutoc2288.averageseeds.web.SeedsSpringApplication
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.*
import kotlin.collections.ArrayList
import kotlin.system.exitProcess

const val maxRequestAttempts = 3
const val requestRetryTimeMinutes = 10
const val packSize = 1000
const val daysCycle = 31
val minDelayBetweenUpdates = TimeUnit.MINUTES.toMillis(15)
val syncTimeZone = ZoneId.of("Europe/Moscow")

val mapper = jsonMapper {
    addModule(KotlinModule.Builder().build())
    serializationInclusion(JsonInclude.Include.NON_NULL)
    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
}

private var previousDay = (LocalDateTime.now(syncTimeZone).toLocalDate().toEpochDay()).toInt()
private var timeOfLastUpdateEnd = 0L
var dayToRead = previousDay // день, за который мы должны читать
val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale("ru"))

val updateScheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
val dbWorker = ThreadPoolExecutor(
    1, 1, 0, TimeUnit.MILLISECONDS, LinkedBlockingQueue(20)
).apply {
    setRejectedExecutionHandler { runnable, threadPoolExecutor ->
        threadPoolExecutor.queue.put(runnable)
    }
}

val pendingSyncUrls = ArrayList<String>()

@Volatile
var dbWorkerException: Exception? = null

@Volatile
var updateTopicsList = ArrayList<SeedsInsertItem>()

@Volatile
var syncTopicsList = ArrayList<SeedsSyncItem>()

fun updateSeeds() {
    val startTime = System.currentTimeMillis()
    println(sdf.format(Date()))
    // номер ячейки текущего дня в БД
    val currentDay = (LocalDateTime.now(syncTimeZone).toLocalDate().toEpochDay()).toInt()
    if (currentDay != previousDay) {
        // Предыдущий день уже закрыт и его можно читать
        dayToRead = currentDay
    }
    println("День $currentDay, предыдущий $previousDay")
    println("Получение количества раздач по разделам...")
    val forumSize = try {
        persistOnResponse { keeperRetrofit.forumSize() }
    } catch (e: Exception) {
        println("Не удалось получить дерево подразделов: $e")
        return
    }.result
    val insertSeeds = {
        val listToProcess = updateTopicsList
        tryOnDbWorker {
            SeedsRepository.appendNewSeeds(listToProcess)
        }
        updateTopicsList = ArrayList()
    }
// обновляем каждый форум
    println("Обновляются сиды в ${forumSize.size} подразделах")

    tryOnDbWorker {
        SeedsRepository.createNewSeedsTable(currentDay != previousDay)
    }
    for (forum in forumSize.keys) {
        val forumTorrents = try {
            persistOnResponse { keeperRetrofit.getForumTorrents(forum) }
        } catch (e: Exception) {
            println("Не удалось получить информацию о разделе $forum: $e")
            continue
        }
        for (torrent in forumTorrents.result) {
            val seedersCount = torrent.value.getOrNull(1) as Int? ?: continue
            val isHighPriority = torrent.value.getOrNull(4) as Int == 2
            updateTopicsList.add(SeedsInsertItem(forum, torrent.key, seedersCount, isHighPriority))
            if (updateTopicsList.size == packSize) {
                insertSeeds.invoke()
            }
        }
    }
    // virtual topic to count main updates
    updateTopicsList.add(SeedsInsertItem(-1, -1, 0, false))
    if (updateTopicsList.isNotEmpty())
        insertSeeds.invoke()

    timeOfLastUpdateEnd = System.currentTimeMillis()    // синхронизация не учитывается во времени обновления

    if (currentDay != previousDay) {
        pendingSyncUrls.clear()
        pendingSyncUrls.addAll(SeedsProperties.syncUrls)
        println("Наступил новый день, синхронизируемся")
        syncSeeds(forumSize.keys)
    } else if (pendingSyncUrls.isNotEmpty()) {
        println("Синхронизация не была выполнена полностью, синхронизируемся")
        syncSeeds(forumSize.keys)
    }

    println("Запись новых данных в базу (будет происходить в фоне)")
    tryOnDbWorker {
        SeedsRepository.commitNewSeeds(currentDay)
        println("Обновление всех разделов завершено за ${((System.currentTimeMillis() - startTime) / 1000 / 60).toInt()} минут")
    }

    previousDay = currentDay
    System.gc()
    System.runFinalization()
}

fun syncSeeds(subsections: Collection<Int>) {
    val currentPendingSyncUrls = ArrayList<String>(pendingSyncUrls)

    val startTime = System.currentTimeMillis()
    for (url in currentPendingSyncUrls) {
        val daysToSync = ArrayList<Int>()
        // чтение из бд, не ставить в воркер!
        val myUpdatesCount = SeedsRepository.getMainUpdates((0..29).toList().toIntArray())
        for (i in myUpdatesCount.indices) {
            if (myUpdatesCount[i] != 24) {
                daysToSync.add(i)
            }
        }
        if (daysToSync.isEmpty()) {
            println("Имеется полная информация за все дни, дальнейшая синхронизация не требуется")
            pendingSyncUrls.clear()
            return
        }
        println("Нужно синхронизировать дни ${daysToSync.joinToString(",")}")
        // номера ячеек для таблицы, в которые записываем полученные данные
        run urlBlock@{
            println("Синхронизация с $url")
            val seedsRetrofit = SeedsRetrofit.forUrl(url)
            val remoteCurrentDay = try {
                responseOrThrow {
                    seedsRetrofit.getCurrentDay()
                }
            } catch (e: Exception) {
                println("Не удалось синхронизироваться с $url, попробуем при следующем обновлении: $e")
                return@urlBlock
            }.currentDay
            if (remoteCurrentDay != dayToRead) {
                println("День в $url отличается от локального, попробуем при следующем обновлении")
                return@urlBlock
            }
            val remoteUpdatesCount = try {
                responseOrThrow {
                    seedsRetrofit.getMainUpdatesCount(daysToSync.joinToString(","))
                }
            } catch (e: Exception) {
                println("Не удалось синхронизироваться с $url, попробуем при следующем обновлении: $e")
                return@urlBlock
            }.mainUpdatesCount!!
            val updatesInDaysToSync = IntArray(daysToSync.size) {
                myUpdatesCount[daysToSync[it]]
            }
            val remoteBetterDays = ArrayList<Int>().apply {
                for (i in remoteUpdatesCount.indices) {
                    if (remoteUpdatesCount[i] <= 24 && remoteUpdatesCount[i] > updatesInDaysToSync[i]) {
                        add(daysToSync[i])
                    }
                }
            }.toIntArray()
            if (remoteBetterDays.isEmpty()) {
                println("$url не обладает более полной информацией о нужных днях")
                pendingSyncUrls.remove(url)
                return@urlBlock
            }
            val updateSeeds = {
                val listToProcess = syncTopicsList
                tryOnDbWorker {
                    SeedsRepository.appendSyncSeeds(listToProcess, remoteBetterDays)
                }
                syncTopicsList = ArrayList()
            }
            val updatesInRemoteBetterDays = IntArray(remoteBetterDays.size) {
                remoteUpdatesCount[daysToSync.indexOf(remoteBetterDays[it])]
            }

            println("Синхронизируем дни ${remoteBetterDays.joinToString(",")} с $url")
            for (subsection in subsections) {
                val remoteSubsectionInfo = try {
                    responseOrThrow {
                        seedsRetrofit.getSingleSubsectionSeeds(subsection, remoteBetterDays.joinToString(","))
                    }
                } catch (e: Exception) {
                    println("Не удалось синхронизироваться с $url, попробуем при следующем обновлении: $e")
                    return@urlBlock
                }.subsections?.get(subsection) ?: continue
                for (remoteTopic in remoteSubsectionInfo) {
                    syncTopicsList.add(
                        SeedsSyncItem(
                            remoteTopic.key,
                            remoteTopic.value.updatesCount ?: updatesInRemoteBetterDays,
                            remoteTopic.value.totalSeeds
                        )
                    )
                    if (syncTopicsList.size == packSize) {
                        updateSeeds.invoke()
                    }
                }
            }
            // обновляем полные обновления
            syncTopicsList.add(
                SeedsSyncItem(
                    -1,
                    updatesInRemoteBetterDays,
                    IntArray(remoteBetterDays.size) { 0 })
            )
            if (syncTopicsList.isNotEmpty())
                updateSeeds.invoke()
            tryOnDbWorker {
                println("Синхронизация с $url завершена")
            }
            pendingSyncUrls.remove(url)
        }
    }
    tryOnDbWorker {
        if (pendingSyncUrls.isEmpty()) {
            println("Синхронизация успешно выполнена")
        } else {
            println("Синхронизация выполнена частично. Повторная попытка после следующего обновления")
        }
        println("Синхронизация завершена за ${((System.currentTimeMillis() - startTime) / 1000 / 60).toInt()} минут")
    }
}

fun main(args: Array<String>) {
    if (args.contains("once")) {
        SeedsProperties.load()
        println("Обновляем сиды один раз и запускаем без API")
        updateSeeds()
        return
    } else {
        if (args.contains("noapi")) {
            println("Запускаем без API")
        } else {
            SpringApplication.run(SeedsSpringApplication::class.java, *args)
        }
    }
    SeedsProperties.load()
    SeedsRepository.prepareDatabase()

    println(
        "Московское время ${
            LocalDateTime.now(syncTimeZone).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))
        }. Если это ошибка, настройте время и часовые пояса на компьютере!"
    )

    val lastUpdateDay = SeedsRepository.getLastUpdateDay()
    println("Сегодняшний день на сервере — $dayToRead, в БД — $lastUpdateDay")
    if (lastUpdateDay < dayToRead) {
        val offset = dayToRead - lastUpdateDay
        if (offset == 1) {
            // если обновлялись вчера, можно просто передвинуть день на 1
            println("Похоже, последний раз база обновлялась вчера — сегодняшний день будет сброшен при обновлении")
            previousDay = lastUpdateDay
        } else {
            // если обновлялись ещё раньше, нужно занулить предыдущие дни и сделать бэкап таблицы на случай ошибки часов
            // таблицу потом надо будет удалить вручную
            println("В последний раз база обновлялась $offset дня назад. Старые данные будут сохранены в таблице " +
                    "TopicsBak, а новые будут смещены в соответствии с текущим днём")
            SeedsRepository.createNewSeedsTable(dayToRead - lastUpdateDay)
            SeedsRepository.commitNewSeeds(dayToRead, true)
        }
    } else if (lastUpdateDay > dayToRead) {
        println("День последнего обновления в БД больше текущего! Этого не должно происходить, сервис будет остановлен")
        exitProcess(1)
    }

    if (args.contains("sync")) {
        println("Синхронизация будет принудительно выполнена после ближайшего обновления")
        pendingSyncUrls.clear()
        pendingSyncUrls.addAll(SeedsProperties.syncUrls)
    }
    if (args.contains("now")) {
        println("Обновляем сиды прямо сейчас")
        updateSeeds()
    }
    val startTime = GregorianCalendar()
    if (startTime.get(Calendar.MINUTE) >= SeedsProperties.updateMinute) {
        // если уже пропустили время то выполним через час
        startTime.add(Calendar.HOUR, 1)
    }
    startTime.set(Calendar.MINUTE, SeedsProperties.updateMinute)
    val delay = startTime.timeInMillis - System.currentTimeMillis()
    println("Ближайшее обновление будет выполнено через ${(delay / 1000 / 60).toInt()} минут")
    updateScheduler.scheduleAtFixedRate({
        if (timeOfLastUpdateEnd + minDelayBetweenUpdates > System.currentTimeMillis()) {
            println(
                "Обновление было пропущено, так как с завершения последнего обновления" +
                        " прошло менее ${TimeUnit.MILLISECONDS.toMinutes(minDelayBetweenUpdates)} минут"
            )
            return@scheduleAtFixedRate
        }
        try {
            updateSeeds()
        } catch (e: Exception) {
            System.err.println("Критическая ошибка при обновлении сидов: $e")
            e.printStackTrace()
        }
    }, delay, 1000 * 60 * 60, TimeUnit.MILLISECONDS)
    //ru.hukutoc2288.averageseeds.updateSeeds()
}

// exception is slow-blow - it will be thrown only after next call of this function
fun tryOnDbWorker(task: () -> Any) {
    if (dbWorkerException != null)
        throw dbWorkerException!!
    dbWorker.submit {
        try {
            task()
        } catch (e: Exception) {
            dbWorker.queue.drainTo(ArrayList())
            if (dbWorkerException == null)
                dbWorkerException = e
        }
    }
}

inline fun <T> persistOnResponse(call: () -> Call<T>): T {
    var currentAttempt = 1
    while (true) {
        if (currentAttempt > 1) {
            println("Повторная попытка $currentAttempt/$maxRequestAttempts")
        }
        try {
            val response = call.invoke().execute()
            if (!response.isSuccessful)
                throw HttpException(response)
            return response.body()!!
        } catch (e: HttpException) {
            val codeType = e.code() / 100
            if (codeType == 4)
                throw e // неразрешимая ошибка типа 404
            println(e.toString())
        } catch (e: Exception) {
            println(e.toString())
        }
        if (currentAttempt++ == maxRequestAttempts) {
            println(
                "Не удалось выполнить запрос за $maxRequestAttempts попыток," +
                        " повторная попытка через $requestRetryTimeMinutes минут"
            )
            Thread.sleep((requestRetryTimeMinutes * 1000 * 60).toLong())
            println("$requestRetryTimeMinutes минут прошло, выполняем запрос снова...")
            currentAttempt = 1
        }
    }
}

inline fun <T> responseOrThrow(call: () -> Call<T>): T {
    var currentAttempt = 1
    while (true) {
        if (currentAttempt > 1) {
            println("Повторная попытка $currentAttempt/$maxRequestAttempts")
        }
        try {
            val response = call.invoke().execute()
            if (!response.isSuccessful)
                throw HttpException(response)
            return response.body()!!
        } catch (e: HttpException) {
            val codeType = e.code() / 100
            if (codeType == 4)
                throw e // неразрешимая ошибка типа 404
            println(e.toString())
        } catch (e: Exception) {
            if (currentAttempt++ == maxRequestAttempts) {
                println("Не удалось выполнить запрос за $maxRequestAttempts попыток, я сдаюсь")
                throw e
            }
            println(e.toString())
        }
    }
}
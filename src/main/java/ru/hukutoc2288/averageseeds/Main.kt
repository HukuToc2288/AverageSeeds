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
import ru.hukutoc2288.averageseeds.utils.daysToCells
import ru.hukutoc2288.averageseeds.web.SeedsSpringApplication
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.*
import kotlin.collections.ArrayList

const val maxRequestAttempts = 3
const val requestRetryTimeMinutes = 10
const val packSize = 1000
const val daysCycle = 31
val syncTimeZone = ZoneId.of("Europe/Moscow")

val mapper = jsonMapper {
    addModule(KotlinModule.Builder().build())
    serializationInclusion(JsonInclude.Include.NON_NULL)
    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
}

private var previousDay = (LocalDateTime.now(syncTimeZone).toLocalDate().toEpochDay() % daysCycle).toInt()
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

fun updateSeeds() {
    val startTime = System.currentTimeMillis()
    println(sdf.format(Date()))
    // номер ячейки текущего дня в БД
    val currentDay = (LocalDateTime.now(syncTimeZone).toLocalDate().toEpochDay() % daysCycle).toInt()
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
    val topicsList = ArrayList<SeedsInsertItem>()
    val insertSeeds = {
        dbWorker.submit {
            SeedsRepository.appendNewSeeds(ArrayList(topicsList), currentDay, currentDay != previousDay)
        }
        topicsList.clear()
    }
    // обновляем каждый форум
    println("Обновляются сиды в ${forumSize.size} подразделах")

    dbWorker.submit {
        SeedsRepository.createNewSeedsTable()
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
            topicsList.add(SeedsInsertItem(forum, torrent.key, seedersCount))
            if (topicsList.size == packSize) {
                insertSeeds.invoke()
            }
        }
    }
    // virtual topic to count main updates
    topicsList.add(SeedsInsertItem(-1, -1, 0))
    if (topicsList.isNotEmpty())
        insertSeeds.invoke()
    println("Запись новых данных в базу (будет происходить в фоне)")

    dbWorker.submit {
        SeedsRepository.commitNewSeeds()
        println("Обновление всех разделов завершено за ${((System.currentTimeMillis() - startTime) / 1000 / 60).toInt()} минут")
    }
    if (currentDay != previousDay) {
        pendingSyncUrls.clear()
        pendingSyncUrls.addAll(SeedsProperties.syncUrls)
        println("Наступил новый день, синхронизируемся")
        syncSeeds(forumSize.keys)
    } else if (pendingSyncUrls.isNotEmpty()) {
        println("Синхронизация не была выполнена полностью, синхронизируемся")
        syncSeeds(forumSize.keys)
    }
    previousDay = currentDay
    System.gc()
    System.runFinalization()
}

// TODO: 23.01.2023 use the same algorithm as in seeds append
fun syncSeeds(subsections: Collection<Int>) {
    val currentPendingSyncUrls = ArrayList<String>(pendingSyncUrls)
    val topicsList = ArrayList<SeedsSyncItem>()
    val updateSeeds = {
        dbWorker.submit {
            SeedsRepository.appendSyncSeeds(ArrayList(topicsList))
        }
        topicsList.clear()
    }

    val startTime = System.currentTimeMillis()
    for (url in currentPendingSyncUrls) {
        val daysToSync = ArrayList<Int>()
        // чтение из бд, не ставить в воркер!
        val myUpdatesCount = SeedsRepository.getMainUpdates(dayToRead, (0..29).toList().toIntArray())
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
            val remoteBetterDays = ArrayList<Int>()
            val updatesInDaysToSync = IntArray(daysToSync.size) {
                myUpdatesCount[daysToSync[it]]
            }
            for (i in remoteUpdatesCount.indices) {
                if (remoteUpdatesCount[i] <= 24 && remoteUpdatesCount[i] > updatesInDaysToSync[i]) {
                    remoteBetterDays.add(daysToSync[i])
                }
            }
            if (remoteBetterDays.isEmpty()) {
                println("$url не обладает более полной информацией о нужных днях")
                pendingSyncUrls.remove(url)
                return@urlBlock
            }
            val updatesInRemoteBetterDays = IntArray(remoteBetterDays.size) {
                remoteUpdatesCount[daysToSync.indexOf(remoteBetterDays[it])]
            }

            val cellsToSync = remoteBetterDays.toIntArray().daysToCells(dayToRead)
            dbWorker.submit {
                SeedsRepository.createSyncSeedsTable(cellsToSync)
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
                    topicsList.add(
                        SeedsSyncItem(
                            remoteTopic.key,
                            remoteTopic.value.updatesCount ?: updatesInRemoteBetterDays,
                            remoteTopic.value.totalSeeds
                        )
                    )
                    if (topicsList.size == packSize) {
                        updateSeeds.invoke()
                    }
                }
            }
            // обновляем полные обновления
            topicsList.add(
                SeedsSyncItem(
                    -1,
                    updatesInRemoteBetterDays,
                    IntArray(remoteBetterDays.size) { 0 })
            )
            if (topicsList.isNotEmpty())
                updateSeeds.invoke()
            println("Запись данных синхронизации в базу (будет происходить в фоне)")
            dbWorker.submit {
                SeedsRepository.commitSyncSeeds(cellsToSync)
                println("Синхронизация с $url завершена")
            }
            pendingSyncUrls.remove(url)
        }
    }
    dbWorker.submit {
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

    if (args.contains("sync")) {
        println("Синхронизация будет принудительно выполнена после ближайшего обновления")
        pendingSyncUrls.clear()
        pendingSyncUrls.addAll(SeedsProperties.syncUrls)
    }
    if (args.contains("now")) {
        println("Обновляем сиды прямо сейчас")
        updateSeeds()
    }
    println(
        "Московское время ${
            LocalDateTime.now(syncTimeZone).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))
        }. Если это ошибка, настройте время и часовые пояса на компьютере!"
    )
    println("Сегодняшний день в БД — $dayToRead")
    val startTime = GregorianCalendar()
    if (startTime.get(Calendar.MINUTE) >= SeedsProperties.updateMinute) {
        // если уже пропустили время то выполним через час
        startTime.add(Calendar.HOUR, 1)
    }
    startTime.set(Calendar.MINUTE, SeedsProperties.updateMinute)
    val delay = startTime.timeInMillis - System.currentTimeMillis()
    println("Ближайшее обновление будет выполнено через ${(delay / 1000 / 60).toInt()} минут")
    updateScheduler.scheduleAtFixedRate({
        try {
            updateSeeds()
        } catch (e: Exception) {
            println("Критическая ошибка при обновлении сидов: $e")
        }
    }, delay, 1000 * 60 * 60, TimeUnit.MILLISECONDS)
    //ru.hukutoc2288.averageseeds.updateSeeds()
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
            println("$requestRetryTimeMinutes прошло, выполняем запрос снова...")
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
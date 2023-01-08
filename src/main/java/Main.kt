import api.keeperRetrofit
import entities.SeedsInsertItem
import retrofit2.Call
import retrofit2.HttpException
import java.io.IOException
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.*
import java.util.concurrent.*


const val maxRequestAttempts = 3
const val requestRetryTimeMinutes = 10
const val startMinute = 3
const val packSize = 1000

var previousDay = (LocalDate.now().toEpochDay() % 31).toInt()
val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale("ru"))

val updateScheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

fun updateSeeds() {
    val startTime = System.currentTimeMillis()
    println(sdf.format(Date()))
    // номер ячейки текущего дня в БД
    val currentDay = (LocalDate.now().toEpochDay() % 31).toInt()
    println("День $currentDay, предыдущий $previousDay")
    println("Получение количества раздач по разделам...")
    val forumSize = try {
        responseOrThrow { keeperRetrofit.forumSize() }
    } catch (e: Exception) {
        println("Не удалось получить дерево подразделов")
        return
    }.result
    val topicsList = ArrayList<SeedsInsertItem>()
    val insertSeeds = {
        if (currentDay == previousDay) {
            // день ещё не кончился, добавляем сиды
            SeedsRepository.incrementSeedsCount(currentDay, topicsList)
        } else {
            // наступил новый день, перезаписываем сиды
            SeedsRepository.setSeedsCount(currentDay, topicsList)
        }
        topicsList.clear()
    }

    // обновляем каждый форум

    println("Обновляются сиды в ${forumSize.size} подразделах")
    for (forum in forumSize.keys) {
        val forumTorrents = try {
            responseOrThrow { keeperRetrofit.getForumTorrents(forum) }
        } catch (e: Exception) {
            println("Не удалось получить информацию о разделе $forum")
            continue
        }
        SeedsRepository.removeUnregisteredTopics(forum, forumTorrents.result.keys)
        for (torrent in forumTorrents.result) {
            val seedersCount = torrent.value.getOrNull(1) as Int? ?: continue
            topicsList.add(SeedsInsertItem(forum, torrent.key, seedersCount))
            if (topicsList.size == packSize) {
                insertSeeds.invoke()
            }
        }
    }
    if (topicsList.isNotEmpty())
        insertSeeds.invoke()
    previousDay = currentDay
    println("Обновление всех разделов завершено за ${((System.currentTimeMillis() - startTime) / 1000 / 60).toInt()} минут")
    System.gc()
    System.runFinalization()
}

fun main(args: Array<String>) {
    if (args.getOrNull(0) == "once") {
        println("Обновляем сиды один раз")
        updateSeeds()
        return
    }
    //SeedsRepository.incrementSeedsCount(1, 1, 1, 1)
    val startTime = GregorianCalendar()
    if (startTime.get(Calendar.MINUTE) >= startMinute) {
        // если уже пропустили время то выполним через час
        startTime.add(Calendar.HOUR, 1)
    }
    startTime.set(Calendar.MINUTE, startMinute)
    val delay = startTime.timeInMillis - System.currentTimeMillis()
    println("Ближайшее обновление будет выполнено через ${(delay / 1000 / 60).toInt()} минут")
    updateScheduler.scheduleAtFixedRate({ updateSeeds() }, delay, 1000 * 60 * 60, TimeUnit.MILLISECONDS)
    //updateSeeds()
}

inline fun <T> responseOrThrow(call: () -> Call<T>): T {
    var currentAttempt = 1
    while (true) {
        if (currentAttempt > 1) {
            println("Повторная попытка $currentAttempt/$maxRequestAttempts")
        }
        try {
            val response = call.invoke().execute()
            if (response.isSuccessful)
                throw HttpException(response)
        } catch (e: HttpException) {
            val codeType = e.code() / 100
            if (codeType == 4)
                throw e // неразрешимая ошибка типа 404
            println(e.toString())
        } catch (e: IOException) {
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
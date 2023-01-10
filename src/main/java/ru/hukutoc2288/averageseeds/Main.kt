package ru.hukutoc2288.averageseeds

import org.springframework.boot.SpringApplication
import retrofit2.Call
import retrofit2.HttpException
import ru.hukutoc2288.averageseeds.api.keeperRetrofit
import ru.hukutoc2288.averageseeds.entities.SeedsInsertItem
import ru.hukutoc2288.averageseeds.web.SeedsSpringApplication
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.*
import java.util.concurrent.*

const val maxRequestAttempts = 3
const val requestRetryTimeMinutes = 10
const val startMinute = 3
const val packSize = 1000
const val daysCycle = 31

var previousDay = (LocalDate.now().toEpochDay() % daysCycle).toInt()
val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale("ru"))

val updateScheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

fun updateSeeds() {
    val startTime = System.currentTimeMillis()
    println(sdf.format(Date()))
    // номер ячейки текущего дня в БД
    val currentDay = (LocalDate.now().toEpochDay() % daysCycle).toInt()
    println("День $currentDay, предыдущий $previousDay")
    println("Получение количества раздач по разделам...")
    val forumSize = try {
        responseOrThrow { keeperRetrofit.forumSize() }
    } catch (e: Exception) {
        println("Не удалось получить дерево подразделов: $e")
        return
    }.result
    val topicsList = ArrayList<SeedsInsertItem>()
    val insertSeeds = {
        SeedsRepository.appendNewSeeds(topicsList)
        topicsList.clear()
    }
    // обновляем каждый форум
    println("Обновляются сиды в ${forumSize.size} подразделах")
    SeedsRepository.createNewSeedsTable()
    for (forum in forumSize.keys) {
        val forumTorrents = try {
            responseOrThrow { keeperRetrofit.getForumTorrents(forum) }
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
    if (topicsList.isNotEmpty())
        insertSeeds.invoke()
    SeedsRepository.commitNewSeeds(currentDay, currentDay != previousDay)
    previousDay = currentDay
    println("Обновление всех разделов завершено за ${((System.currentTimeMillis() - startTime) / 1000 / 60).toInt()} минут")
    System.gc()
    System.runFinalization()
}

fun main(args: Array<String>) {
    if (args.contains("once")) {
        println("Обновляем сиды один раз и запускаем без API")
        updateSeeds()
        return
    } else {
        if (args.contains("noapi")) {
            println("Запускаем без API")
        } else {
            SpringApplication.run(SeedsSpringApplication::class.java)
        }
    }
    //ru.hukutoc2288.averageseeds.SeedsRepository.incrementSeedsCount(1, 1, 1, 1)
    val startTime = GregorianCalendar()
    if (startTime.get(Calendar.MINUTE) >= startMinute) {
        // если уже пропустили время то выполним через час
        startTime.add(Calendar.HOUR, 1)
    }
    startTime.set(Calendar.MINUTE, startMinute)
    val delay = startTime.timeInMillis - System.currentTimeMillis()
    println("Ближайшее обновление будет выполнено через ${(delay / 1000 / 60).toInt()} минут")
    updateScheduler.scheduleAtFixedRate({ updateSeeds() }, delay, 1000 * 60 * 60, TimeUnit.MILLISECONDS)
    //ru.hukutoc2288.averageseeds.updateSeeds()
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
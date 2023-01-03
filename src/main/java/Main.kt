import api.keeperRetrofit
import entities.ForumTopicsInfo
import entities.SeedsInsertItem
import retrofit2.Call
import retrofit2.HttpException
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.*
import java.util.concurrent.*


const val maxRequestAttempts = 5
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

fun main() {
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
    updateSeeds()
}

inline fun <T> responseOrThrow(catForumTree: () -> Call<T>): T {
    for (attempt in 1..maxRequestAttempts) {
        try {
            if (attempt > 1) {
                println("Повторная попытка $attempt/$maxRequestAttempts")
            }
            val response = catForumTree.invoke().execute()
            if (response.code() == 404) {
                throw HttpException(response)
            }
            return response.body()!!
        } catch (e: HttpException) {
            throw e
        } catch (e: Exception) {
            e.printStackTrace()
            if (attempt == maxRequestAttempts) {
                println("Не удалось выполнить запрос за $maxRequestAttempts попыток, я сдаюсь")
                throw e
            }
        }
    }
    throw ArrayIndexOutOfBoundsException()
}

fun createDbWorker(): ExecutorService {
    return ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue()).apply {
        setRejectedExecutionHandler { runnable, executor ->
            executor.queue.put(runnable)
            if (executor.isShutdown) {
                throw RejectedExecutionException("Task $runnable rejected from $executor")
            }
        }
    }
}
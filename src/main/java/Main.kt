import api.keeperRetrofit
import entities.SeedsInsertItem
import retrofit2.Call
import retrofit2.HttpException
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.*
import java.util.concurrent.*


const val maxRequestAttempts = 5
const val startMinute = 34
const val packSize = 1000

var previousDay = (LocalDate.now().toEpochDay() % 31).toInt()
val sdf = SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

val updateScheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
lateinit var dbWorker: ExecutorService

fun updateSeeds() {
    val startTime = System.currentTimeMillis()
//    println("Получение дерева подразделов...")
//    val forumTree = try {
//        responseOrThrow { keeperRetrofit.catForumTree() }
//    } catch (e: Exception) {
//        println("Не удалось получить дерево подразделов")
//        return
//    }.result
    // номер ячейки текущего дня в БД
    println(sdf.format(Date()))
    val currentDay = (LocalDate.now().toEpochDay() % 31).toInt()
    println("День $currentDay, предыдущий $previousDay")
    println("Получение количества раздач по разделам...")
    val forumSize = try {
        responseOrThrow { keeperRetrofit.forumSize() }
    } catch (e: Exception) {
        println("Не удалось получить дерево подразделов")
        return
    }.result
    var topicsList = ArrayList<SeedsInsertItem>()
    dbWorker = createDbWorker()
    val insertSeeds = {
        // пускаем работу с бд в параллель
        dbWorker.submit {
            if (currentDay == previousDay) {
                // день ещё не кончился, добавляем сиды
                SeedsRepository.incrementSeedsCount(currentDay, topicsList)
            } else {
                // наступил новый день, перезаписываем сиды
                SeedsRepository.setSeedsCount(currentDay, topicsList)
            }
        }
        topicsList = ArrayList()
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
        dbWorker.submit {
            SeedsRepository.removeUnregisteredTopics(forum, forumTorrents.result.keys)
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
    dbWorker.shutdown()
    val dbWorkerDone = dbWorker.awaitTermination(5, TimeUnit.MINUTES)
    if (!dbWorkerDone) {
        println("Превышено время ожидания завершения обработчика БД. Этого не должно происходить")
        dbWorker.shutdownNow()
    }
    previousDay = currentDay
    System.gc()
    System.runFinalization()
    println("Обновление всех разделов завершено за ${((System.currentTimeMillis() - startTime) / 1000 / 60).toInt()} минут")
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
    //updateSeeds()
}

fun <T> responseOrThrow(catForumTree: () -> Call<T>): T {
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
    return ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue(10)).apply {
        setRejectedExecutionHandler { runnable, executor ->
            executor.queue.put(runnable)
            if (executor.isShutdown) {
                throw RejectedExecutionException("Task $runnable rejected from $executor")
            }
        }
    }
}
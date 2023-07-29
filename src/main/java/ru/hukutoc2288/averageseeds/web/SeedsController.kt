package ru.hukutoc2288.averageseeds.web

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import ru.hukutoc2288.averageseeds.utils.SeedsRepository
import ru.hukutoc2288.averageseeds.mapper
import ru.hukutoc2288.averageseeds.dayToRead
import ru.hukutoc2288.averageseeds.daysCycle
import ru.hukutoc2288.averageseeds.entities.seeds.CurrentDayResponseBody
import ru.hukutoc2288.averageseeds.entities.seeds.VersionResponseBody
import ru.hukutoc2288.averageseeds.utils.CachingIterator
import ru.hukutoc2288.averageseeds.utils.RangeUtils
import java.io.OutputStream

private const val SUBSECTION_START = -2

@RestController
class SeedsController {

    // длина строки, если перечислить все дни через запятую, + некоторый запас
    // -2 т.к. дни начинаются с нуля + запрещаем показывать текущий день
    var daysStringLimit = ((daysCycle - 2).toString().length + 1) * (daysCycle - 1)

    @GetMapping("/seeds", produces = ["application/json"])
    fun greeting(
        @RequestParam(name = "subsections", required = false) subsections: IntArray?,
        @RequestParam(name = "days", required = false) daysToRequestString: String?
    ): ResponseEntity<StreamingResponseBody> {

        val daysToRequest = if (daysToRequestString == null) {
            (0..daysCycle - 2).toList().toIntArray()
        } else {
            if (daysToRequestString.length > daysStringLimit) {
                throw IllegalArgumentException("Строка дней слишком длинная: текущее ограничение $daysStringLimit," +
                        " длина строки в запросе ${daysToRequestString.length}. Ограничение рассчитано автоматически, если его не хватает," +
                        " скорее всего это ошибка клиента")
            }
            RangeUtils.parse(daysToRequestString, 0, daysCycle - 2)
        }
        // TODO: есть смысл ввести ограничения и на кол-во подразделов
        daysToRequest ?: throw IllegalArgumentException("некорректный диапазон дней: $daysToRequestString")
        val mainUpdatesCount = SeedsRepository.getMainUpdates(daysToRequest)
        val topicsIterator =
            SeedsRepository.getSeedsInSubsections(subsections, mainUpdatesCount, daysToRequest)
        val responseBody = StreamingResponseBody { responseStream ->
            try {
                // building answer manually, as we may run OOM on potato PC
                responseStream.write("{\"mainUpdatesCount\":")
                responseStream.write(mapper.writeValueAsBytes(mainUpdatesCount))
                responseStream.write(",\"subsections\":{")
                var currentSubsection = SUBSECTION_START
                while (topicsIterator.hasNext()) {
                    val currentEntry = topicsIterator.next()
                    if (currentSubsection != currentEntry.subsection) {
                        if (currentSubsection == SUBSECTION_START) {
                            // first subsection, no closing needed
                            currentSubsection = currentEntry.subsection
                            responseStream.write("\"$currentSubsection\":{")
                        } else {
                            // start new subsections
                            currentSubsection = currentEntry.subsection
                            responseStream.write("},\"$currentSubsection\":{")
                        }
                    } else {
                        responseStream.write(",")
                    }
                    responseStream.write("\"${currentEntry.topicId}\":")
                    responseStream.write(mapper.writeValueAsBytes(currentEntry.seedsData))
                    responseStream.flush()
                }
                responseStream.write("}}")  // close last subsection and subsections list
                // write message field here if needed
                responseStream.write("}")
                responseStream.flush()
            } catch (e: Exception) {
                e.printStackTrace()
                throw e
            } finally {
                if (topicsIterator is CachingIterator)
                    topicsIterator.closeResources()
                try {
                    responseStream.close()
                } finally {

                }
            }
        }
        return ResponseEntity.ok().body(responseBody)
    }

    @GetMapping("/currentDay", produces = ["application/json"])
    fun getCurrentDay(): String {
        return mapper.writeValueAsString(
            CurrentDayResponseBody(
                dayToRead
            )
        )
    }

    @GetMapping("/version", produces = ["application/json"])
    fun getVersion(): String {
        return mapper.writeValueAsString(VersionResponseBody())
    }
}

private fun OutputStream.write(s: String) {
    write(s.toByteArray())
}
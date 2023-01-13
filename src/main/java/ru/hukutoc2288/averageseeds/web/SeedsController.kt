package ru.hukutoc2288.averageseeds.web

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import ru.hukutoc2288.averageseeds.SeedsRepository
import ru.hukutoc2288.averageseeds.mapper
import ru.hukutoc2288.averageseeds.dayToRead
import ru.hukutoc2288.averageseeds.entities.web.CurrentDayResponseBody
import java.io.OutputStream

private const val SUBSECTION_START = -2

@RestController
class SeedsController {

    @GetMapping("/seeds", produces = ["application/json"])
    fun greeting(
        @RequestParam(name = "subsections", required = false) subsections: IntArray?
    ): ResponseEntity<StreamingResponseBody> {
        val mainUpdatesCount = SeedsRepository.getMainUpdates(dayToRead)
        val topicsIterator = SeedsRepository.getSeedsInSubsections(dayToRead, subsections, mainUpdatesCount)
        val responseBody = StreamingResponseBody { responseStream ->
            // building answer manually, as we may run OOM on potato PC
            responseStream.write("{\"success\":true,\"mainUpdatesCount\":")
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
            responseStream.close()
        }
        return ResponseEntity.ok().body(responseBody)
    }

    @GetMapping("/currentDay", produces = ["application/json"])
    fun getCurrentDay(): String {
        return mapper.writeValueAsString(
            CurrentDayResponseBody(
                true,
                dayToRead
            )
        )
    }
}

private fun OutputStream.write(s: String) {
    write(s.toByteArray())
}
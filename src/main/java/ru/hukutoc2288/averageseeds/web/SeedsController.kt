package ru.hukutoc2288.averageseeds.web

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import ru.hukutoc2288.averageseeds.SeedsRepository
import ru.hukutoc2288.averageseeds.entities.web.SeedsResponseBody
import ru.hukutoc2288.averageseeds.mapper
import ru.hukutoc2288.averageseeds.previousDay

@RestController
class SeedsController {

    @GetMapping("/seeds", produces = ["application/json"])
    fun greeting(@RequestParam(name = "subsections", required = true) subsections: IntArray): String {
        val mainUpdatesCount = SeedsRepository.getMainUpdates(
            previousDay
        )
        return mapper.writeValueAsString(
            SeedsResponseBody(
                true,
                message = null,
                mainUpdatesCount,
                SeedsRepository.getSeedsInSubsections(
                    previousDay,
                    subsections,
                    mainUpdatesCount,
                )
            )
        )
    }
}
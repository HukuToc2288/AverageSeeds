package ru.hukutoc2288.averageseeds.web

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jsonMapper
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import ru.hukutoc2288.averageseeds.SeedsRepository
import ru.hukutoc2288.averageseeds.entities.web.SeedsResponseBody
import ru.hukutoc2288.averageseeds.previousDay
import java.io.File

@RestController
class SeedsController {
    private val mapper = jsonMapper {
        addModule(KotlinModule.Builder().build())
        serializationInclusion(JsonInclude.Include.NON_NULL)
    }

    @GetMapping("/seeds", produces = ["application/json"])
    fun greeting(@RequestParam(name = "subsections", required = true) subsections: IntArray): String {
        return mapper.writeValueAsString(
            SeedsResponseBody(
                true,
                message = null,
                SeedsRepository.getSeedsInSubsections(
                    previousDay,
                    subsections,
                    SeedsRepository.getMainUpdates(
                        previousDay
                    )
                )
            )
        )
    }
}
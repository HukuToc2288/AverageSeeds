package ru.hukutoc2288.averageseeds.web

import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import ru.hukutoc2288.averageseeds.entities.web.SeedsResponseBody

@RestController
class SeedsController {
    @GetMapping("/seeds", produces = arrayOf("application/json"))
    fun greeting(@RequestParam(name = "subsections", required = true) name: IntArray): SeedsResponseBody {
        return SeedsResponseBody(true)
    }
}
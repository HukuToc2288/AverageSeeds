package ru.hukutoc2288.averageseeds.web

import jakarta.servlet.RequestDispatcher
import jakarta.servlet.http.HttpServletRequest
import org.springframework.boot.web.servlet.error.ErrorController
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import ru.hukutoc2288.averageseeds.entities.seeds.SeedsResponseBody
import ru.hukutoc2288.averageseeds.mapper

@RestController
class SeedsErrorController : ErrorController {

    @GetMapping(
        "/error",
        produces = ["application/json"]
    )
    fun handleError(request: HttpServletRequest): String {
        val exception = request.getAttribute(RequestDispatcher.ERROR_EXCEPTION) as java.lang.Exception?
        val errorMessage = exception?.cause?.message
        return mapper.writeValueAsString(
            SeedsResponseBody(
                if (errorMessage.isNullOrEmpty())
                    HttpStatus.valueOf(
                        request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE) as Int
                    ).reasonPhrase
                else
                    errorMessage
            )
        )
    }
}
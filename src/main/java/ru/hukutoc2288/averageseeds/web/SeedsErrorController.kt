package ru.hukutoc2288.averageseeds.web

import jakarta.servlet.RequestDispatcher
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.boot.web.servlet.error.ErrorController
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import ru.hukutoc2288.averageseeds.SeedsRepository
import ru.hukutoc2288.averageseeds.entities.web.SeedsResponseBody

@RestController
class SeedsErrorController : ErrorController {

    @GetMapping(
        "/error",
        produces = ["application/json"]
    )
    fun handleError(request: HttpServletRequest): SeedsResponseBody {
        val errorMessage = request.getAttribute(RequestDispatcher.ERROR_MESSAGE) as String?
        return SeedsResponseBody(
            false,
            if (errorMessage.isNullOrEmpty())
                HttpStatus.valueOf(
                    request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE) as Int
                ).reasonPhrase
            else
                errorMessage
        )
    }
}
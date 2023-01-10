package ru.hukutoc2288.averageseeds.entities.web

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
class SeedsResponseBody(
    var success: Boolean,
    var message: String? = null,
    var subsections: HashMap<Int, SubsectionBody>? = null
)
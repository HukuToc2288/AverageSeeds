package ru.hukutoc2288.averageseeds.entities.web

class CurrentDayResponseBody(
    var success: Boolean,
    var currentDay: Int,
    var message: String? = null,
)
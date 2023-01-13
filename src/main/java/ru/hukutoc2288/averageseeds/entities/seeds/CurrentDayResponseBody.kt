package ru.hukutoc2288.averageseeds.entities.seeds

class CurrentDayResponseBody(
    var success: Boolean,
    var currentDay: Int,
    var message: String? = null,
)
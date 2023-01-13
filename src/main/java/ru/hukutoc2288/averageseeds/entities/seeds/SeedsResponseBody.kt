package ru.hukutoc2288.averageseeds.entities.seeds

class SeedsResponseBody(
    var success: Boolean,
    var message: String? = null,
    var mainUpdatesCount: IntArray? = null,
    var subsections: Map<Int, Map<Int,TopicResponseItem>>? = null,
)
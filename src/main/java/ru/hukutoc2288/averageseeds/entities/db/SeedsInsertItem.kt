package ru.hukutoc2288.averageseeds.entities.db

class SeedsInsertItem(
    val forumId: Int,
    val topicId: Int,
    val seedsCount: Int,
    val highPriority: Boolean
)
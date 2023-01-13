package ru.hukutoc2288.averageseeds.entities

import ru.hukutoc2288.averageseeds.entities.web.TopicResponseItem

class TopicItem(
    val subsection: Int,
    val topicId: Int,
    val seedsData: TopicResponseItem
)
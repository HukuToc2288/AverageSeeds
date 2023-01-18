package ru.hukutoc2288.averageseeds.entities.db

import ru.hukutoc2288.averageseeds.entities.seeds.TopicResponseItem

class TopicItem(
    val subsection: Int,
    val topicId: Int,
    val seedsData: TopicResponseItem
)
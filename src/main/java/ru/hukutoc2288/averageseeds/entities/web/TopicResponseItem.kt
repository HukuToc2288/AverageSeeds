package ru.hukutoc2288.averageseeds.entities.web

import com.fasterxml.jackson.annotation.JsonProperty

class TopicResponseItem(
    @JsonProperty("s") val totalSeeds: IntArray,
    @JsonProperty("u") val updatesCount: IntArray?
)
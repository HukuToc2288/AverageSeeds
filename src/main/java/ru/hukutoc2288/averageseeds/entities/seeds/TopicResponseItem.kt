package ru.hukutoc2288.averageseeds.entities.seeds

import com.fasterxml.jackson.annotation.JsonProperty

class TopicResponseItem(
    @JsonProperty("s") val totalSeeds: IntArray,
    @JsonProperty("u") val updatesCount: IntArray?
)
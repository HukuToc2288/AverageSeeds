package ru.hukutoc2288.averageseeds.entities

import com.fasterxml.jackson.annotation.JsonProperty

class ForumSize (
    @JsonProperty("result") val result: HashMap<Int,List<Long>>
)
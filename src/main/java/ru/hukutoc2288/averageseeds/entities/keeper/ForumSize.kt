package ru.hukutoc2288.averageseeds.entities.keeper

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.*

class ForumSize (
    @JsonProperty("result") val result: TreeMap<Int, List<Long>>
)
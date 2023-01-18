package ru.hukutoc2288.averageseeds.entities.keeper

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.*

class ForumTree(
    @JsonProperty("result") val result: ForumTreeResults
) {
    // категория (c) -> форум (f) -> подфорум (f)
    class ForumTreeResults(
        @JsonProperty("c") val categories: TreeMap<Int, String>,
        @JsonProperty("f") val forums: TreeMap<Int, String>,
        @JsonProperty("tree") val tree: TreeMap<Int, TreeMap<Int, List<Int>>>
    ) {

    }
}
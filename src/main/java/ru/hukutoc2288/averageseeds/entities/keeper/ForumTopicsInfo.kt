package ru.hukutoc2288.averageseeds.entities.keeper

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.TreeMap

class ForumTopicsInfo(
    @JsonProperty("total_size_bytes") val totalSize: Long,
    //  "tor_status", "seeders", "reg_time", "tor_size_bytes", "keeping_priority", "keepers", "seeder_last_seen", "info_hash"
    @JsonProperty("result") val result: TreeMap<Int, List<Any>>
) {
}
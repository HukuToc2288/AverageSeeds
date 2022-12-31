package entities.keeper

import com.fasterxml.jackson.annotation.JsonProperty

class ForumTopicsInfo(
    @JsonProperty("total_size_bytes") val totalSize: Long,
    //  "tor_status", "seeders", "reg_time", "tor_size_bytes", "keeping_priority", "keepers", "seeder_last_seen", "info_hash"
    @JsonProperty("result") val result: HashMap<Int, List<Any>>
) {
}
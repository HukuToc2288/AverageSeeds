package ru.hukutoc2288.averageseeds.api.seeds

import retrofit2.Call
import retrofit2.http.GET
import retrofit2.http.Query
import retrofit2.http.Url
import ru.hukutoc2288.averageseeds.entities.seeds.CurrentDayResponseBody
import ru.hukutoc2288.averageseeds.entities.seeds.SeedsResponseBody

interface SeedsApi {

    @GET("currentDay")
    fun getCurrentDay(): Call<CurrentDayResponseBody>

    @GET("seeds")
    fun getSingleSubsectionSeeds(
        @Query("subsections") subsection: Int,
        @Query("days") daysString: String
    ): Call<SeedsResponseBody>

    @GET("seeds")
    fun getMainUpdatesCount(
        @Query("days") daysString: String
    ): Call<SeedsResponseBody>
}
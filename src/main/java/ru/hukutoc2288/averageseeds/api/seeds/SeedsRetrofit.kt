package ru.hukutoc2288.averageseeds.api.seeds

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory

var seedsRetrofit = createSeedsApi()
    private set

private fun createSeedsApi(): SeedsApi {
    val clientBuilder = OkHttpClient.Builder()
    //.addInterceptor(HttpLoggingInterceptor().setLevel(HttpLoggingInterceptor.Level.BASIC))

    return Retrofit.Builder()
        .baseUrl("https://localhost/")  // stub
        .addConverterFactory(JacksonConverterFactory.create(ObjectMapper().apply {
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)
        }))
        .client(clientBuilder.build())
        .build()
        .create(SeedsApi::class.java)

}
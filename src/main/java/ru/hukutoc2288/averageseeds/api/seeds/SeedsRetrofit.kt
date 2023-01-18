package ru.hukutoc2288.averageseeds.api.seeds

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory
import ru.hukutoc2288.averageseeds.mapper

object SeedsRetrofit {
    fun forUrl(baseUrl: String): SeedsApi {
        val clientBuilder = OkHttpClient.Builder()
        //.addInterceptor(HttpLoggingInterceptor().setLevel(HttpLoggingInterceptor.Level.BASIC))

        return Retrofit.Builder()
            .baseUrl(baseUrl)
            .addConverterFactory(JacksonConverterFactory.create(mapper))
            .client(clientBuilder.build())
            .build()
            .create(SeedsApi::class.java)

    }
}
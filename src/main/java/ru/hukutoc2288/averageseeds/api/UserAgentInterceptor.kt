package ru.hukutoc2288.averageseeds.api

import okhttp3.Interceptor
import okhttp3.Response
import kotlin.Throws
import java.io.IOException

/* This interceptor adds a custom User-Agent. */
class UserAgentInterceptor(private val userAgent: String) : Interceptor {
    @Throws(IOException::class)
    override fun intercept(chain: Interceptor.Chain): Response {
        val originalRequest = chain.request()
        val requestWithUserAgent = originalRequest.newBuilder()
            .header("User-Agent", userAgent)
            .build()
        return chain.proceed(requestWithUserAgent)
    }
}
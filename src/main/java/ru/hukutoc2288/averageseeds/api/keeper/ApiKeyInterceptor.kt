package ru.hukutoc2288.averageseeds.api.keeper

import okhttp3.HttpUrl
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import java.io.IOException

class ApiKeyInterceptor(private val apiKey: String?) : Interceptor {
    @Throws(IOException::class)
    override fun intercept(chain: Interceptor.Chain): Response {
        var request: Request = chain.request()
        val url: HttpUrl = request.url.newBuilder().addQueryParameter("apiKey", apiKey).build()
        request = request.newBuilder().url(url).build()
        return chain.proceed(request)
    }
}
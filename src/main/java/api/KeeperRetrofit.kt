package api

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory
import ru.hukutoc2288.jvmtlo.api.ApiKeyInterceptor
import ru.hukutoc2288.jvmtlo.api.UserAgentInterceptor
import java.net.InetSocketAddress
import java.net.Proxy


var keeperRetrofit = createKeeperApi()
    private set

private fun createKeeperApi(): KeeperApi {
    val clientBuilder = OkHttpClient.Builder()
        .addInterceptor(UserAgentInterceptor("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"))
        .addInterceptor(ApiKeyInterceptor(""))
        //.addInterceptor(HttpLoggingInterceptor().setLevel(HttpLoggingInterceptor.Level.BASIC))

    return Retrofit.Builder()
        .baseUrl("https://api.t-ru.org/")
        .addConverterFactory(JacksonConverterFactory.create(ObjectMapper().apply {
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)
        }))
        .client(clientBuilder.build())
        .build()
        .create(KeeperApi::class.java)

}

fun rebuildKeeperApi() {
    keeperRetrofit = createKeeperApi()
}
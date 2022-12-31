package ru.hukutoc2288.jvmtlo.api

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import ru.hukutoc2288.jvmtlo.entities.config.ProxyConfigProxy
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory
import ru.hukutoc2288.jvmtlo.utils.ConfigRepository
import java.net.InetSocketAddress
import java.net.Proxy

val keeperCookieJar = SingleUrlCookieJar()

var keeperRetrofit = createKeeperApi()
    private set

private fun createKeeperApi(): KeeperApi {
    val clientBuilder = OkHttpClient.Builder()
        .addInterceptor(UserAgentInterceptor("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"))
        .addInterceptor(ApiKeyInterceptor(ConfigRepository.trackerConfig.apiKey))
        .addInterceptor(HttpLoggingInterceptor().setLevel(HttpLoggingInterceptor.Level.BASIC))
        .cookieJar(keeperCookieJar)

    with(ConfigRepository.proxyConfig) {
        if (proxyApi) {
            // TODO: 01.12.2022 поддержка нескольких прокси
            val selectedProxy = proxies[0]
            val proxyType =
                if (selectedProxy.type == ProxyConfigProxy.ProxyType.SOCKS5) {
                    Proxy.Type.SOCKS
                } else {
                    Proxy.Type.HTTP
                }
            val proxy = Proxy(
                proxyType, InetSocketAddress(
                    selectedProxy.hostname,
                    selectedProxy.port
                )
            )
            clientBuilder.proxy(proxy)
        }
    }
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
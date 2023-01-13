package ru.hukutoc2288.averageseeds.utils

import java.io.*
import java.nio.charset.StandardCharsets
import java.util.*
import kotlin.system.exitProcess

object SeedsProperties {

    val updateMinuteKey = "updateMinute"
    val syncUrlsKey = "syncUrls"

    var updateMinute: Int = 0
    lateinit var syncUrls: Collection<String>


    fun load() {
        val properties = initDefaults()
        try {
            properties.load(InputStreamReader(FileInputStream("seeds.properties"), StandardCharsets.UTF_8))
            setVariablesFromProperties(properties)
        } catch (e: FileNotFoundException) {
            write(properties)
            setVariablesFromProperties(properties)
        } catch (e: Exception) {
            System.err.println("Файл seeds.properties испорчен. Исправьте ошибки в файле или удалите его, чтобы пересоздать")
            exitProcess(1)
        }
    }

    private fun setVariablesFromProperties(properties: Properties){
        updateMinute = (properties[updateMinuteKey] as String).toInt()
        syncUrls = (properties[updateMinuteKey] as String).split(';').map {
            val urlBuilder = StringBuilder()
            if (!it.startsWith("http://") && !it.startsWith("https://")) {
                urlBuilder.append("http://")
            }
            urlBuilder.append(it)
            if (!it.endsWith('/'))
                urlBuilder.append('/')
            urlBuilder.toString()
        }
    }

    private fun initDefaults(): Properties {
        val properties = Properties()
        properties[updateMinuteKey] = "3"
        properties[syncUrlsKey] = ""
        return properties
    }

    private fun write(properties: Properties) {
        try {
            properties.store(OutputStreamWriter(FileOutputStream("seeds.properties"), StandardCharsets.UTF_8), null)
        } catch (e: Exception) {
            System.err.println("Файл seeds.properties отсутствует и не может быть создан. Предоставьте право на запись или создайте файл вручную")
            exitProcess(1)
        }
    }
}
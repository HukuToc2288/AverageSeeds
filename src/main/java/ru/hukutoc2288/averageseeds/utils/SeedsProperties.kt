package ru.hukutoc2288.averageseeds.utils

import java.io.*
import java.nio.charset.StandardCharsets
import java.util.*
import kotlin.system.exitProcess

object SeedsProperties {

    private val updateMinuteKey = "updateMinute"
    private val syncUrlsKey = "syncUrls"
    private val dbUrlKey = "dbUrl"

    var updateMinute: Int = 0
    lateinit var syncUrls: List<String>
    lateinit var dbUrl: String


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

    private fun setVariablesFromProperties(properties: Properties) {
        updateMinute = (properties[updateMinuteKey] as String).toInt()
        syncUrls = run {
            val rawList = (properties[syncUrlsKey] as String).split(';')
            val returnList = ArrayList<String>()
            for (rawUrl in rawList) {
                if (rawUrl.isEmpty())
                    continue
                val urlBuilder = StringBuilder()
                if (!rawUrl.startsWith("http://") && !rawUrl.startsWith("https://")) {
                    urlBuilder.append("http://")
                }
                urlBuilder.append(rawUrl)
                if (!rawUrl.endsWith('/'))
                    urlBuilder.append('/')
                returnList.add(urlBuilder.toString())
            }
            returnList
        }
        dbUrl = properties[dbUrlKey] as String
    }

    private fun initDefaults(): Properties {
        val properties = Properties()
        properties[updateMinuteKey] = "3"
        properties[syncUrlsKey] = ""
        properties[dbUrlKey] = "seeds"
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
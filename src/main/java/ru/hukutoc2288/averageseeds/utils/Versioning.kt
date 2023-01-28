package ru.hukutoc2288.averageseeds.utils

object Versioning {
    const val majorVersion = 0
    const val minorVersion = 4
    const val patchVersion = 7
    const val versionTag = ""

    val version = "$majorVersion.$minorVersion.$patchVersion" + if (versionTag.isEmpty()) "" else "-$versionTag"
}
package ru.hukutoc2288.averageseeds.utils

object Versioning {
    const val majorVersion = 1
    const val minorVersion = 2
    const val patchVersion = 0
    const val versionTag = ""

    val version = "$majorVersion.$minorVersion.$patchVersion" + if (versionTag.isEmpty()) "" else "-$versionTag"
}
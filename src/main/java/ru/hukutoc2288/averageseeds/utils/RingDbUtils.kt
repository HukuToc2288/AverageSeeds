package ru.hukutoc2288.averageseeds.utils

import ru.hukutoc2288.averageseeds.dayToRead
import ru.hukutoc2288.averageseeds.daysCycle

fun IntArray.cellsToDays(currentDay: Int): IntArray {
    return IntArray(this.size) {
        (daysCycle - currentDay + it + 1) % daysCycle
    }
}

fun IntArray.daysToCells(currentDay: Int): IntArray {
    return IntArray(this.size) {
        (daysCycle + currentDay - it - 1) % daysCycle
    }
}
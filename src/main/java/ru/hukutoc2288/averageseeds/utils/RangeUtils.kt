package ru.hukutoc2288.averageseeds.utils

object RangeUtils {

    // собираем дни из строки
    fun parse(string: String, min: Int, max: Int): IntArray? {
        if (min < 0)
            throw IllegalArgumentException("min < 0")
        else if (min > max) {
            throw IllegalArgumentException("min > max")
        }
        if (!string.matches("^[0-9-,]+$".toRegex())) {
            // разрешаем только цифры, дефис и запятую
            return null
        }
        val maxDigits = max.toString().length   // максимальное кол-во цифр в числе дня
        string.indexOf('-').let { firstHyphenIndex ->
            if (firstHyphenIndex >= 0) {
                // есть дефис, парсим как диапазон
                if (string.contains(',')) {
                    // дефис и запятая в одной строке, такое мы парсить не будем
                    return null
                }
                if (firstHyphenIndex == 0
                    || firstHyphenIndex == string.length - 1
                    || firstHyphenIndex != string.lastIndexOf('-')
                ) {
                    // дефис в начале, в конце, или встречается больше одного раза
                    return null
                }
                val rangeString = string.split('-')
                val start = checkAndParseInt(rangeString[0], min, max, maxDigits) ?: return null
                val end = checkAndParseInt(rangeString[1], min, max, maxDigits) ?: return null
                if (start > end) {
                    return null
                }
                return (start..end).toList().toIntArray()
            } else {
                // нет дефисов, парсим как цифры через запятую
                val daysString = string.split(',')
                val daysInt = ArrayList<Int>(daysString.size)
                for (dayString in daysString) {
                    checkAndParseInt(dayString, min, max, maxDigits)?.let { dayIntNotNull ->
                        daysInt.add(dayIntNotNull)
                    }
                }
                if (daysInt.isEmpty()) {
                    return null
                }
                return daysInt.toIntArray()
            }
        }
    }

    private fun checkAndParseInt(value: String, min: Int, max: Int, maxDigits: Int): Int? {
        if (value.length > maxDigits || value.isEmpty()) {
            // быстро отсекаем лишнее + защищаем от превышения размера Int
            return null
        }
        val valueInt = value.toInt()    // Это безопасно, т.к. ранее уже проверили, что строка состоит из цифр
        if (valueInt < min || valueInt > max) {
            return null
        }
        return valueInt
    }
}
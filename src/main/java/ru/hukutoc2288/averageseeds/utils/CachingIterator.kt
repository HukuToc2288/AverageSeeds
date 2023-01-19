package ru.hukutoc2288.averageseeds.utils

import java.sql.ResultSet

// work with DB results as Iterable
abstract class CachingIterator<T>(private val resultSet: ResultSet) : Iterator<T> {
    private var resources = ArrayList<AutoCloseable>()
    private var cachedHasNext = false
    override fun hasNext(): Boolean {
        if (!cachedHasNext)
            cachedHasNext = resultSet.next()
        if (!cachedHasNext) {
            resultSet.close()
            closeResources()
        }
        return cachedHasNext
    }

    override fun next(): T {
        if (!cachedHasNext)
            resultSet.next()
        cachedHasNext = false
        return processResult(resultSet)
    }

    fun addResource(resource: AutoCloseable){
        resources.add(resource)
    }

    fun closeResources(){
        for (r in resources)
            r.close()
    }

    abstract fun processResult(resultSet: ResultSet): T
}
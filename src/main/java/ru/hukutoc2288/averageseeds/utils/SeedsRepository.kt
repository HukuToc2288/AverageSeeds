package ru.hukutoc2288.averageseeds.utils

import com.zaxxer.hikari.HikariDataSource
import ru.hukutoc2288.averageseeds.dayToRead
import ru.hukutoc2288.averageseeds.daysCycle
import ru.hukutoc2288.averageseeds.entities.db.SeedsInsertItem
import ru.hukutoc2288.averageseeds.entities.db.SeedsSyncItem
import ru.hukutoc2288.averageseeds.entities.db.TopicItem
import ru.hukutoc2288.averageseeds.entities.seeds.TopicResponseItem
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Collections
import kotlin.math.min
import kotlin.system.exitProcess

object SeedsRepository {
    private const val currentDayKey = "currentDay"
    private const val dataVersionKey = "dataVersion"
    private const val targetDataVersion = 1

    private val updateConnectionPool = HikariDataSource().apply {
        jdbcUrl = "jdbc:postgresql:${SeedsProperties.dbUrl}"
        addDataSourceProperty("cachePrepStmts", "true");
        addDataSourceProperty("prepStmtCacheSize", "250");
        addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
        maximumPoolSize = 1
        isAutoCommit = false
        idleTimeout = 60_000

    }

    private val apiConnectionPool = HikariDataSource().apply {
        jdbcUrl = "jdbc:postgresql:${SeedsProperties.dbUrl}"
        addDataSourceProperty("cachePrepStmts", "true");
        addDataSourceProperty("prepStmtCacheSize", "250");
        addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
        maximumPoolSize = 4
        isAutoCommit = false
        isReadOnly = true
        idleTimeout = 60_000
    }

    fun prepareDatabase() {
        var connection: Connection? = null
        var statement: Statement? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            statement.addBatch("CREATE TABLE IF NOT EXISTS Topics (${buildTableStructureQuery(true)})")
            statement.addBatch("CREATE INDEX IF NOT EXISTS ss_index ON Topics(ss)")
            statement.addBatch(
                "CREATE TABLE IF NOT EXISTS Variables (" +
                        "key VARCHAR NOT NULL PRIMARY KEY," +
                        "value INT NOT NULL)"
            )
            statement.addBatch(
                "INSERT INTO Variables VALUES" +
                        " ('$currentDayKey', $dayToRead)," +
                        " ('$dataVersionKey', 0)" +
                        " ON CONFLICT DO NOTHING"
            )
            statement.executeBatch()
            connection.commit()
            statement.close()
            connection.close()

            updateDataToTargetVersion()

            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            statement.addBatch("INSERT INTO Topics(id,ss,rg,hp) VALUES (-1,1,1,false) ON CONFLICT DO NOTHING")
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()
        }
    }


    fun getCurrentDataVersion(): Int {
        var connection: Connection? = null
        var statement: Statement? = null
        var resultSet: ResultSet? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            resultSet = statement.executeQuery("SELECT value FROM Variables WHERE key='$dataVersionKey' LIMIT 1")
            if (!resultSet.next())
                throw SQLException("Data version not acquired, this doesn't suppose to happen!")
            return resultSet.getInt(1)
        } finally {
            resultSet?.close()
            statement?.close()
            connection?.close()
        }
    }

    private fun updateDataToTargetVersion() {
        var currentDataVersion = getCurrentDataVersion()
        if (currentDataVersion == targetDataVersion)
            return
        if (currentDataVersion > targetDataVersion) {
            println("Версия базы данных выше, чем требуемая! Этого не должно происходить, сервис будет остановлен")
            exitProcess(1)
        }
        var connection: Connection? = null
        var statement: Statement? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            if (currentDataVersion < 1) {
                // version 1 - added registration date column
                statement.addBatch("ALTER TABLE Topics ADD COLUMN IF NOT EXISTS rg BIGINT NOT NULL DEFAULT 0")
                statement.addBatch("ALTER TABLE Topics ALTER COLUMN rg DROP DEFAULT")
                currentDataVersion = 1
            }
            statement.addBatch("UPDATE Variables SET value=$currentDataVersion WHERE key='$dataVersionKey'")
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()
        }
    }

    fun getLastUpdateDay(): Int {
        var connection: Connection? = null
        var statement: Statement? = null
        var resultSet: ResultSet? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            resultSet = statement.executeQuery("SELECT value FROM Variables WHERE key='$currentDayKey' LIMIT 1")
            if (!resultSet.next())
                throw SQLException("Last update day not acquired, this doesn't suppose to happen!")
            return resultSet.getInt(1)
        } finally {
            resultSet?.close()
            statement?.close()
            connection?.close()
        }
    }

    fun createNewSeedsTable(isNewDay: Boolean) {
        createNewSeedsTable(if (isNewDay) 1 else 0)
    }

    fun createNewSeedsTable(offset: Int) {
        val startTime = System.currentTimeMillis()
        var connection: Connection? = null
        var statement: Statement? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            statement.addBatch("DROP TABLE IF EXISTS TopicsNew")
            statement.addBatch("CREATE TABLE TopicsNew (${buildTableStructureQuery(false)})")
            // copy all values into new table if synced in less than 31 days
            if (offset < daysCycle)
                statement.addBatch(buildTransferSeedsQuery(offset))
            // build primary key on new data
            statement.addBatch("ALTER TABLE TopicsNew ADD PRIMARY KEY (id)")

            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()

            println("create new table done in ${((System.currentTimeMillis() - startTime) / 1000).toInt()} seconds")
            appendWasteTime = 0
        }
    }

    var appendWasteTime: Long = 0
    fun appendNewSeeds(topics: MutableCollection<SeedsInsertItem>) {
        appendWasteTime -= System.currentTimeMillis()
        var connection: Connection? = null
        var statement: PreparedStatement? = null
        val datesOfPresentTopics = getRegistrationDatesByIds(topics.map { it.topicId })
        val topicsToRemove = ArrayList<Int>()
        for (topic in topics) {
            // если тема есть, но дата регистрации поменялась, надо сбросить сиды,
            // что достигается удалением темы, и последующей вставкой её как новой
            // оставляем дату регистрации 0 для совместимости
            if (datesOfPresentTopics.containsKey(topic.topicId) &&
                datesOfPresentTopics[topic.topicId] != topic.registrationDate &&
                datesOfPresentTopics[topic.topicId] != 0L
            ) {
                topicsToRemove.add(topic.topicId)
            }
        }
        try {
            connection = updateConnectionPool.connection
            if (topicsToRemove.isNotEmpty())
                connection.createStatement()
                    .execute("DELETE FROM TopicsNew WHERE id in (${topicsToRemove.joinToString(",")})")
            statement = connection.prepareStatement(
                "INSERT INTO TopicsNew(id,ss,rg,hp,sc,uc) VALUES (?,?,?,?,?,1)" +
                        " ON CONFLICT(id) DO UPDATE SET " +
                        "ss=excluded.ss," +
                        "rg=excluded.rg," +
                        "hp=excluded.hp," +
                        "sc=TopicsNew.sc+excluded.sc," +
                        "uc=TopicsNew.uc+excluded.uc"
            )
            for (topic in topics) {
                statement.setInt(1, topic.topicId)
                statement.setInt(2, topic.forumId)
                statement.setLong(3, topic.registrationDate)
                statement.setBoolean(4, topic.highPriority)
                // really big overhead to database if we have to use int instead of smallint on transactions
                // and yes, disk space is really matters on potato PC
                // or not?
                statement.setInt(5, min(topic.seedsCount, 1000))
                statement.addBatch()
            }
            statement.executeBatch()
            statement.close()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()
            appendWasteTime += System.currentTimeMillis()
        }
    }

    private fun getRegistrationDatesByIds(ids: Collection<Int>): Map<Int, Long> {
        var connection: Connection? = null
        var statement: Statement? = null
        var resultSet: ResultSet? = null
        try {
            connection = apiConnectionPool.connection
            statement = connection.createStatement()
            resultSet = statement.executeQuery("SELECT id,rg FROM Topics WHERE id IN (${ids.joinToString(",")})")
            val dates = HashMap<Int, Long>()
            while (resultSet.next()) {
                dates[resultSet.getInt(1)] = resultSet.getLong(2)
            }
            return dates
        } finally {
            resultSet?.close()
            statement?.close()
            connection?.close()
        }
    }

    fun commitNewSeeds(currentDay: Int, backup: Boolean = false) {
        val startTime = System.currentTimeMillis()
        var connection: Connection? = null
        var statement: Statement? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            if (backup) {
                // drop index to free its name and save space
                statement.addBatch("DROP INDEX ss_index")
                // keep old data so we can do something with it
                // FIXME: 08.02.2023 it will fail if we already have backup
                statement.addBatch("ALTER TABLE Topics RENAME TO TopicsBak")
                // rename primary key matching table name
                statement.addBatch("ALTER TABLE TopicsBak RENAME CONSTRAINT topics_pkey TO topicsbak_pkey")
            } else {
                // just drop old table
                statement.addBatch("DROP TABLE Topics")
            }
            // set new topics as current topics
            statement.addBatch("ALTER TABLE TopicsNew RENAME TO Topics")
            // create ss index
            statement.addBatch("CREATE INDEX ss_index ON Topics(ss)")
            // rename primary key matching table name
            statement.addBatch("ALTER TABLE Topics RENAME CONSTRAINT topicsnew_pkey TO topics_pkey")
            // update day in the same transaction
            statement.addBatch("UPDATE Variables SET value=$currentDay WHERE key='$currentDayKey'")
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()
            println("done in ${((System.currentTimeMillis() - startTime) / 1000).toInt()} seconds")
            println("append waste time is ${((appendWasteTime) / 1000).toInt()} seconds")
        }
    }

    fun appendSyncSeeds(topics: Collection<SeedsSyncItem>, daysToSync: IntArray) {
        if (topics.isEmpty())
            return
        val valuesCount = topics.first().updatesCount.size  // количество значений вставляемых в БД
        var connection: Connection? = null
        var statement: Statement? = null
        try {
            connection = updateConnectionPool.connection
//            // no INSERT OR REPLACE in psql (why??)
//            // tFIXME: 21.01.2023 seems to be not good solution
//            connection.createStatement().execute(
//                "DELETE FROM TopicsNew WHERE id IN (${
//                    topics.joinToString(",") {
//                        it.topicId.toString()
//                    }
//                })"
//            )
            var insertSyncQuery = "UPDATE TopicsNew SET "
            for (day in daysToSync) {
                insertSyncQuery += "u$day=?,s$day=?,"
            }
            insertSyncQuery = insertSyncQuery.dropLast(1)
            insertSyncQuery += "WHERE id = ?"
            statement = connection.prepareStatement(insertSyncQuery)
            // эта цыганская магия позволит нам вставлять значения в нужные ячейки и не таскать с собой индексы
            for (topic in topics) {
                statement.setInt(valuesCount * 2 + 1, topic.topicId)
                for (i in 0 until valuesCount) {
                    statement.setInt(i * 2 + 1, topic.updatesCount[i])
                    // really big overhead to database if we have to use int instead of smallint on transactions
                    // and yes, disk space is really matters on potato PC
                    statement.setInt(i * 2 + 2, min(topic.totalSeeds[i], 24 * 1000))
                }
                statement.addBatch()
            }
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()
        }
    }

    fun getMainUpdates(daysToRequest: IntArray): IntArray {
        var connection: Connection? = null
        var statement: Statement? = null
        var resultSet: ResultSet? = null
        val columnsToSelect = StringBuilder()
        columnsToSelect.append("id,ss")
        for (d in daysToRequest) {
            columnsToSelect.append(",u$d")
        }
        try {
            connection = apiConnectionPool.connection
            statement = connection.createStatement()
            resultSet = statement.executeQuery("SELECT $columnsToSelect FROM TOPICS WHERE id=-1 LIMIT 1")
            val mainUpdatesCount = IntArray(daysToRequest.size)
            if (!resultSet.next())
                throw SQLException("Main updates not acquired, this doesn't suppose to happen!")
            for ((posInArray, day) in daysToRequest.withIndex()) {
                mainUpdatesCount[posInArray] = resultSet.getInt("u$day")
            }
            return mainUpdatesCount
        } finally {
            resultSet?.close()
            statement?.close()
            connection?.close()
        }
    }

    fun getSeedsInSubsections(
        subsections: IntArray?,
        mainUpdatesCount: IntArray,
        daysToRequest: IntArray
    ): Iterator<TopicItem> {
        if (subsections?.isNotEmpty() != true || daysToRequest.isEmpty())
            return Collections.emptyIterator()
//        if (mainUpdatesCount.size != 30)
//            throw IllegalArgumentException("mainUpdatesCount size must be 30")
        val connection = apiConnectionPool.connection
        var statement: Statement? = null
        var resultSet: ResultSet? = null
        val columnsToSelect = StringBuilder()
        columnsToSelect.append("id,ss")
        for (d in daysToRequest) {
            columnsToSelect.append(",s$d,u$d")
        }
        try {
            statement = connection.createStatement()
            statement.fetchSize = 20
            resultSet = statement.executeQuery(
                "SELECT $columnsToSelect FROM Topics WHERE ss IN (${subsections.joinToString(",")})" +
                        " ORDER BY ss"
            )
            return object : CachingIterator<TopicItem>(resultSet) {
                override fun processResult(resultSet: ResultSet): TopicItem {
                    val updatesCount = IntArray(daysToRequest.size)
                    val totalSeedsCount = IntArray(daysToRequest.size)
                    for ((posInArray, day) in daysToRequest.withIndex()) {
                        updatesCount[posInArray] = resultSet.getInt("u$day")
                        totalSeedsCount[posInArray] = resultSet.getInt("s$day")
                    }
                    return TopicItem(
                        resultSet.getInt("ss"),
                        resultSet.getInt("id"),
                        TopicResponseItem(
                            totalSeedsCount,
                            if (mainUpdatesCount.contentEquals(updatesCount))
                                null
                            else
                                updatesCount
                        )
                    )
                }
            }.apply {
                addResource(statement)
                addResource(connection)
            }
        } finally {
            // all will be closed in iterator
        }
    }

    private fun buildTableStructureQuery(addPkey: Boolean): String {
        var query = "id INT " +
                (if (addPkey) "PRIMARY KEY " else "") +
                "NOT NULL," +
                "ss SMALLINT NOT NULL," +
                "rg BIGINT NOT NULL," +
                "hp BOOLEAN NOT NULL," +
                "sc SMALLINT NOT NULL DEFAULT 0"
        for (i in 0 until daysCycle - 1) {
            query += ",s$i SMALLINT NOT NULL DEFAULT 0"
        }
        query += ",uc SMALLINT NOT NULL DEFAULT 0"
        for (i in 0 until daysCycle - 1) {
            query += ",u$i SMALLINT NOT NULL DEFAULT 0"
        }
        return query
    }

    private fun buildTransferSeedsQuery(offset: Int): String {
        if (offset < 0)
            throw IllegalArgumentException("offset less than zero: $offset")
        if (offset >= daysCycle) {
            throw IllegalArgumentException("offset not less than daysCycle: $offset when daysCycle is $daysCycle")
        }
        var valuesToInsert = "id,ss,rg,hp,sc"
        for (i in 0 until daysCycle - 1) {
            valuesToInsert += ",s$i"
        }
        valuesToInsert += ",uc"
        for (i in 0 until daysCycle - 1) {
            valuesToInsert += ",u$i"
        }
        var valuesToSelect = "id,ss,rg,hp"
        valuesToSelect += ",0".repeat(offset)
        for (i in -1 until daysCycle - offset - 1) {
            valuesToSelect += if (i == -1)
                ",sc"
            else
                ",s$i"
        }
        valuesToSelect += ",0".repeat(offset)
        for (i in -1 until daysCycle - offset - 1) {
            valuesToSelect += if (i == -1)
                ",uc"
            else
                ",u$i"
        }
        return "INSERT INTO TopicsNew($valuesToInsert) SELECT $valuesToSelect FROM Topics"
    }
}
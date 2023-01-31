package ru.hukutoc2288.averageseeds.utils

import com.zaxxer.hikari.HikariDataSource
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

object SeedsRepository {

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
            val createTableQueryBuilder = StringBuilder(100 + 66 * daysCycle)
            createTableQueryBuilder.append(
                "CREATE TABLE IF NOT EXISTS Topics (" +
                        "id INT NOT NULL PRIMARY KEY," +
                        "ss SMALLINT NOT NULL"
            )
            for (i in 0 until daysCycle) {
                createTableQueryBuilder.append(",s$i SMALLINT NOT NULL DEFAULT 0")
            }
            for (i in 0 until daysCycle) {
                createTableQueryBuilder.append(",u$i SMALLINT NOT NULL DEFAULT 0")
            }
            createTableQueryBuilder.append(')')
            statement.addBatch(createTableQueryBuilder.toString())
            statement.addBatch("INSERT INTO Topics(id,ss) VALUES (-1,1) ON CONFLICT DO NOTHING")
            statement.addBatch("CREATE INDEX IF NOT EXISTS ss_index ON Topics(ss)")
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()
        }
    }

    fun createNewSeedsTable(currentDay: Int, isNewDay: Boolean) {
        val startTime = System.currentTimeMillis()
        var connection: Connection? = null
        var statement: Statement? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            statement.addBatch("DROP TABLE IF EXISTS TopicsNew")
            statement.addBatch(
                "CREATE TABLE TopicsNew (LIKE Topics INCLUDING CONSTRAINTS INCLUDING DEFAULTS)"
            )
            val insertSelectQuery = if (isNewDay) {
                // zeroify current day on fly
                var valuesToSelect = "id,ss"
                for (day in 0 until daysCycle) {
                    valuesToSelect += if (day == currentDay)
                        ",0"
                    else
                        ",s$day"
                }
                for (day in 0 until daysCycle) {
                    valuesToSelect += if (day == currentDay)
                        ",0"
                    else
                        ",u$day"
                }
                valuesToSelect
            } else {
                // just copy all values
                "*"
            }
            // copy all values into new table
            statement.addBatch("INSERT INTO TopicsNew SELECT $insertSelectQuery FROM Topics")
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
    fun appendNewSeeds(topics: Collection<SeedsInsertItem>, currentDay: Int) {
        appendWasteTime -= System.currentTimeMillis()
        var connection: Connection? = null
        var statement: PreparedStatement? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.prepareStatement(
                "INSERT INTO TopicsNew(id,ss,s$currentDay,u$currentDay) VALUES (?,?,?,1)" +
                        " ON CONFLICT(id) DO UPDATE SET " +
                        "ss=excluded.ss," +
                        "s$currentDay=TopicsNew.s$currentDay+excluded.s$currentDay," +
                        "u$currentDay=TopicsNew.u$currentDay+excluded.u$currentDay"
            )
            for (topic in topics) {
                statement.setInt(1, topic.topicId)
                statement.setInt(2, topic.forumId)
                // really big overhead to database if we have to use int instead of smallint on transactions
                // and yes, disk space is really matters on potato PC
                statement.setInt(3, min(topic.seedsCount, 1000))
                statement.addBatch()
            }
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()
            appendWasteTime += System.currentTimeMillis()
        }
    }

    fun commitNewSeeds() {
        val startTime = System.currentTimeMillis()
        var connection: Connection? = null
        var statement: Statement? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            // drop old topics
            statement.addBatch("DROP TABLE Topics")
            // set new topics as current topics
            statement.addBatch("ALTER TABLE TopicsNew RENAME TO Topics")
            // create ss index
            statement.addBatch("CREATE INDEX IF NOT EXISTS ss_index ON Topics(ss)")
            // rename primary key matching table name
            statement.addBatch("ALTER TABLE Topics RENAME CONSTRAINT topicsnew_pkey TO topics_pkey")
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()
            println("done in ${((System.currentTimeMillis() - startTime) / 1000).toInt()} seconds")
            println("append waste time is ${((appendWasteTime) / 1000).toInt()} seconds")
        }
    }

    fun createSyncSeedsTable(tableDaysToSync: IntArray) {
        var connection: Connection? = null
        var statement: Statement? = null
        try {
            connection = updateConnectionPool.connection
            statement = connection.createStatement()
            statement.addBatch("DROP TABLE IF EXISTS TopicsNew")
            val createTableQuery = StringBuilder()
            createTableQuery.append(
                "CREATE TABLE TopicsNew (id INT NOT NULL PRIMARY KEY"
            )
            for (day in tableDaysToSync) {
                createTableQuery.append(",u$day SMALLINT,s$day SMALLINT")
            }
            createTableQuery.append(")")
            statement.addBatch(createTableQuery.toString())
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
            connection?.close()
        }
    }

    fun appendSyncSeeds(topics: Collection<SeedsSyncItem>, currentDay: Int, daysToSync: IntArray) {
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
            val cellsToUpdate = daysToSync.daysToCells(currentDay)
            var insertSyncQuery = "UPDATE TopicsNew SET "
            for (day in cellsToUpdate) {
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

    fun commitSyncSeeds(tableDaysToSync: IntArray) {
        var connection: Connection? = null
        var insertStatement: Statement? = null
        try {
            connection = updateConnectionPool.connection
            insertStatement = connection.createStatement()
            val insertSyncQuery = StringBuilder()
            insertSyncQuery.append("UPDATE Topics SET ")
            for (day in tableDaysToSync) {
                insertSyncQuery.append("u$day=t.u$day,s$day=t.s$day,")
            }
            insertSyncQuery.setCharAt(insertSyncQuery.length - 1, ' ')
            insertSyncQuery.append("FROM (SELECT * FROM TopicsNew) as t WHERE Topics.id = t.id")
            insertStatement.addBatch(insertSyncQuery.toString())
            insertStatement.executeBatch()
            connection.commit()
        } finally {
            insertStatement?.close()
            connection?.close()
            var dropStatement: Statement? = null
            try {
                connection = updateConnectionPool.connection
                dropStatement = connection.createStatement()
                dropStatement.execute("DROP TABLE TopicsNew")
                connection.commit()
            } finally {
                dropStatement?.close()
                connection?.close()
            }
        }
    }

    fun getMainUpdates(currentDay: Int, daysToRequest: IntArray): IntArray {
        var connection: Connection? = null
        var statement: Statement? = null
        var resultSet: ResultSet? = null
        val columnsToSelect = StringBuilder()
        columnsToSelect.append("id,ss")
        val cellsToRequest = daysToRequest.daysToCells(currentDay)
        for (d in cellsToRequest) {
            columnsToSelect.append(",u$d")
        }
        try {
            connection = apiConnectionPool.connection
            statement = connection.createStatement()
            resultSet = statement.executeQuery("SELECT $columnsToSelect FROM TOPICS WHERE id=-1 LIMIT 1")
            val mainUpdatesCount = IntArray(cellsToRequest.size)
            if (!resultSet.next())
                throw SQLException("Main updates not acquired, this doesn't suppose to happen!")
            for ((posInArray, day) in cellsToRequest.withIndex()) {
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
        currentDay: Int,
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
        val cellsToRequest = daysToRequest.daysToCells(currentDay)
        for (d in cellsToRequest) {
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
                    val updatesCount = IntArray(cellsToRequest.size)
                    val totalSeedsCount = IntArray(cellsToRequest.size)
                    for ((posInArray, day) in cellsToRequest.withIndex()) {
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
        }
    }
}
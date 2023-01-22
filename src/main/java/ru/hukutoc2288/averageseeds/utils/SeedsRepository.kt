package ru.hukutoc2288.averageseeds.utils

import ru.hukutoc2288.averageseeds.daysCycle
import ru.hukutoc2288.averageseeds.entities.db.SeedsInsertItem
import ru.hukutoc2288.averageseeds.entities.db.SeedsSyncItem
import ru.hukutoc2288.averageseeds.entities.db.TopicItem
import ru.hukutoc2288.averageseeds.entities.seeds.TopicResponseItem
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Collections
import kotlin.math.min

object SeedsRepository {

    // TODO: 22.01.2023 use pool

    private val connection: Connection = DriverManager.getConnection(
        "jdbc:postgresql:${SeedsProperties.dbUrl}",
    ).apply {
        autoCommit = false
    }

//    private val apiConnection: Connection = DriverManager.getConnection(
//        "jdbc:sqlite:files/seeds.db",
//        SQLiteConfig().apply {
//            setJournalMode(SQLiteConfig.JournalMode.WAL)
//            setReadOnly(true)
//            setReadUncommited(true)
//        }.toProperties()
//    ).apply {
//        autoCommit = false
//
//    }

    private val apiConnection: Connection = DriverManager.getConnection(
        "jdbc:postgresql:${SeedsProperties.dbUrl}",
    ).apply {
        autoCommit = false
        isReadOnly = true
    }

    init {
        var statement: Statement? = null
        try {
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
            var st = "alter table topics "
            for (i in 0 until daysCycle) {
                st +=
                    "    alter column s$i  type smallint,"
            }
            st = st.dropLast(1)
            createTableQueryBuilder.append(')')
            statement.addBatch(createTableQueryBuilder.toString())
            statement.addBatch(st)
            statement.addBatch("INSERT INTO Topics(id,ss) VALUES (-1,1) ON CONFLICT DO NOTHING")
            statement.addBatch("CREATE INDEX IF NOT EXISTS ss_index ON Topics(ss)")
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
        }
    }

    fun createNewSeedsTable() {
        val startTime = System.currentTimeMillis()
        var statement: Statement? = null
        try {
            statement = connection.createStatement()
            statement.addBatch("DROP TABLE IF EXISTS TopicsNew")
            statement.addBatch("CREATE TABLE TopicsNew (LIKE Topics INCLUDING ALL)")
            statement.addBatch("INSERT INTO TopicsNew SELECT * FROM Topics")
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()

            println("done in ${((System.currentTimeMillis() - startTime) / 1000).toInt()} seconds")
            appendWasteTime = 0
        }
    }

    var appendWasteTime: Long = 0
    fun appendNewSeeds(topics: Collection<SeedsInsertItem>, day: Int, isNewDay: Boolean) {
        appendWasteTime -= System.currentTimeMillis()
        var statement: PreparedStatement? = null
        try {
            statement = connection.prepareStatement(
                "INSERT INTO TopicsNew(id,ss,u$day,s$day) VALUES (?,?,1,?)" +
                        " ON CONFLICT(id) DO UPDATE SET ss=excluded.ss, " + (
                        if (isNewDay)
                            "u$day=1, s$day=excluded.s$day" // новый день, сбрасываем сиды и обновления
                        else
                            "u$day=TopicsNew.u$day+1, s$day=TopicsNew.s$day+excluded.s$day" // день ещё идёт, добавляем сиды и обновления
                        )
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
            appendWasteTime += System.currentTimeMillis()
            statement?.close()
        }
    }

    fun commitNewSeeds() {
        val startTime = System.currentTimeMillis()
        var insertStatement: Statement? = null
        try {
            insertStatement = connection.createStatement()
            insertStatement.execute("DROP TABLE Topics")
            insertStatement.addBatch("ALTER TABLE TopicsNew RENAME TO Topics")
            insertStatement.addBatch("CREATE INDEX ss_index ON Topics(ss)")
            insertStatement.executeBatch()
            connection.commit()
        } finally {
            insertStatement?.close()
            println("done in ${((System.currentTimeMillis() - startTime) / 1000).toInt()} seconds")
            println("append waste time is ${((appendWasteTime) / 1000).toInt()} seconds")
        }
    }

    fun createSyncSeedsTable(tableDaysToSync: IntArray) {
        // TODO: 19.01.2023 WIP, make table temporary when done
        var statement: Statement? = null
        try {
            statement = connection.createStatement()
            statement.addBatch("DROP TABLE IF EXISTS TopicsNew")
            val createTableQuery = StringBuilder()
            createTableQuery.append(
                "CREATE TABLE TopicsNew (" +
                        "id INT NOT NULL PRIMARY KEY"
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
        }
    }

    fun appendSyncSeeds(topics: Collection<SeedsSyncItem>) {
        if (topics.isEmpty())
            return
        val valuesCount = topics.first().updatesCount.size  // количество значений вставляемых в БД
        var statement: PreparedStatement? = null
        try {
            // no INSERT OR REPLACE in psql (why??)
            // FIXME: 21.01.2023 seems to be not good solution
            connection.createStatement().execute(
                "DELETE FROM TopicsNew WHERE id IN (${
                    topics.joinToString(",") {
                        it.topicId.toString()
                    }
                })"
            )
            statement = connection.prepareStatement(
                "INSERT INTO TopicsNew VALUES (?${",?".repeat(valuesCount * 2)})"
            )
            // эта цыганская магия позволит нам вставлять значения в нужные ячейки и не таскать с собой индексы
            for (topic in topics) {
                statement.setInt(1, topic.topicId)
                for (i in 0 until valuesCount) {
                    statement.setInt(i * 2 + 2, topic.updatesCount[i])
                    // really big overhead to database if we have to use int instead of smallint on transactions
                    // and yes, disk space is really matters on potato PC
                    statement.setInt(i * 2 + 3, min(topic.totalSeeds[i], 24 * 1000))
                }
                statement.addBatch()
            }
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
        }
    }

    fun commitSyncSeeds(tableDaysToSync: IntArray) {
        var insertStatement: Statement? = null
        try {
            insertStatement = connection.createStatement()
            val insertSyncQuery = StringBuilder()
            insertSyncQuery.append("UPDATE Topics SET ")
            for (day in tableDaysToSync) {
                // FIXME: 17.01.2023 это явно может делаться проще и я чего-то не знаю
                insertSyncQuery.append("u$day=t.u$day,s$day=t.s$day,")
            }
            insertSyncQuery.setCharAt(insertSyncQuery.length - 1, ' ')
            insertSyncQuery.append("FROM (SELECT * FROM TopicsNew) as t WHERE Topics.id = t.id")
            insertStatement.addBatch(insertSyncQuery.toString())
            insertStatement.executeBatch()
            connection.commit()
        } finally {
            insertStatement?.close()
            var dropStatement: Statement? = null
            try {
                dropStatement = connection.createStatement()
                dropStatement.execute("DROP TABLE TopicsNew")
                connection.commit()
            } finally {
                dropStatement?.close()
            }
        }
    }

    fun getMainUpdates(currentDay: Int, daysToRequest: IntArray): IntArray {
        var statement: Statement? = null
        var resultSet: ResultSet? = null
        val columnsToSelect = StringBuilder()
        columnsToSelect.append("id,ss")
        val cellsToRequest = daysToRequest.daysToCells(currentDay)
        for (d in cellsToRequest) {
            columnsToSelect.append(",u$d")
        }
        try {
            statement = apiConnection.createStatement()
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
        var statement: Statement? = null
        var resultSet: ResultSet? = null
        val columnsToSelect = StringBuilder()
        columnsToSelect.append("id,ss")
        val cellsToRequest = daysToRequest.daysToCells(currentDay)
        for (d in cellsToRequest) {
            columnsToSelect.append(",s$d,u$d")
        }
        try {
            statement = apiConnection.createStatement()
            statement.fetchSize = 5
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
            }
        } finally {
        }
    }
}
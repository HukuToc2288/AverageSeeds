package ru.hukutoc2288.averageseeds.utils

import org.sqlite.SQLiteConfig
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

object SeedsRepository {

    private val connection: Connection = DriverManager.getConnection(
        "jdbc:sqlite:files/seeds.db",
        SQLiteConfig().apply {
            setJournalMode(SQLiteConfig.JournalMode.OFF)
        }.toProperties()
    )

    init {
        Class.forName("org.sqlite.JDBC")
        connection.autoCommit = false
        var statement: Statement? = null
        try {
            statement = connection.createStatement()
            statement.addBatch(
                "CREATE TABLE IF NOT EXISTS Topics (" +
                        "id INT NOT NULL PRIMARY KEY," +
                        "ss INT NOT NULL," +
                        "s0  INT,\n" +
                        "s1  INT,\n" +
                        "s2  INT,\n" +
                        "s3  INT,\n" +
                        "s4  INT,\n" +
                        "s5  INT,\n" +
                        "s6  INT,\n" +
                        "s7  INT,\n" +
                        "s8  INT,\n" +
                        "s9  INT,\n" +
                        "s10 INT,\n" +
                        "s11 INT,\n" +
                        "s12 INT,\n" +
                        "s13 INT,\n" +
                        "s14 INT,\n" +
                        "s15 INT,\n" +
                        "s16 INT,\n" +
                        "s17 INT,\n" +
                        "s18 INT,\n" +
                        "s19 INT,\n" +
                        "s20 INT,\n" +
                        "s21 INT,\n" +
                        "s22 INT,\n" +
                        "s23 INT,\n" +
                        "s24 INT,\n" +
                        "s25 INT,\n" +
                        "s26 INT,\n" +
                        "s27 INT,\n" +
                        "s28 INT,\n" +
                        "s29 INT,\n" +
                        "s30 INT,\n" +
                        "u0  INT,\n" +
                        "u1  INT,\n" +
                        "u2  INT,\n" +
                        "u3  INT,\n" +
                        "u4  INT,\n" +
                        "u5  INT,\n" +
                        "u6  INT,\n" +
                        "u7  INT,\n" +
                        "u8  INT,\n" +
                        "u9  INT,\n" +
                        "u10 INT,\n" +
                        "u11 INT,\n" +
                        "u12 INT,\n" +
                        "u13 INT,\n" +
                        "u14 INT,\n" +
                        "u15 INT,\n" +
                        "u16 INT,\n" +
                        "u17 INT,\n" +
                        "u18 INT,\n" +
                        "u19 INT,\n" +
                        "u20 INT,\n" +
                        "u21 INT,\n" +
                        "u22 INT,\n" +
                        "u23 INT,\n" +
                        "u24 INT,\n" +
                        "u25 INT,\n" +
                        "u26 INT,\n" +
                        "u27 INT,\n" +
                        "u28 INT,\n" +
                        "u29 INT,\n" +
                        "u30 INT" +
                        ")"
            )
            connection.commit()
        } finally {
            statement?.close()
        }
    }

    fun createNewSeedsTable() {
        var statement: Statement? = null
        try {
            statement = connection.createStatement()
            statement.addBatch("DROP TABLE IF EXISTS temp.TopicsNew;")
            statement.addBatch(
                "CREATE TEMPORARY TABLE TopicsNew (" +
                        "id INT NOT NULL PRIMARY KEY," +
                        "ss INT NOT NULL," +
                        "se INT" +
                        ")"
            )
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
        }
    }

    fun appendNewSeeds(topics: Collection<SeedsInsertItem>) {
        var statement: PreparedStatement? = null
        try {
            statement = connection.prepareStatement(
                "INSERT OR REPLACE INTO temp.TopicsNew(id,ss,se) VALUES (?,?,?)"
            )
            for (topic in topics) {
                statement.setInt(1, topic.topicId)
                statement.setInt(2, topic.forumId)
                statement.setInt(3, topic.seedsCount)
                statement.addBatch()
            }
            statement.executeBatch()
            connection.commit()
        } finally {
            statement?.close()
        }
    }

    fun commitNewSeeds(day: Int, isNewDay: Boolean) {
        var insertStatement: Statement? = null
        try {
            insertStatement = connection.createStatement()
            insertStatement.addBatch("DELETE FROM Topics WHERE Topics.id NOT IN (SELECT TopicsNew.id FROM temp.TopicsNew)")
            insertStatement.addBatch(
                "INSERT INTO Topics(id,ss,u$day,s$day) SELECT id,ss,1,se FROM temp.TopicsNew WHERE TRUE" +
                        " ON CONFLICT(id) DO UPDATE SET ss=excluded.ss, " + (
                        if (isNewDay)
                            "u$day=1, s$day=excluded.s$day" // новый день, сбрасываем сиды и обновления
                        else
                            "u$day=u$day+1, s$day=s$day+excluded.s$day" // день ещё идёт, добавляем сиды и обновления
                        )
            )
            insertStatement.executeBatch()
            connection.commit()
        } finally {
            insertStatement?.close()
            var dropStatement: Statement? = null
            try {
                dropStatement = connection.createStatement()
                dropStatement.execute("DROP TABLE temp.TopicsNew")
                connection.commit()
            } finally {
                dropStatement?.close()
            }
        }
    }

    fun createSyncSeedsTable(tableDaysToSync: IntArray) {
        var statement: Statement? = null
        try {
            statement = connection.createStatement()
            statement.addBatch("DROP TABLE IF EXISTS temp.TopicsNew")
            val createTableQuery = StringBuilder()
            createTableQuery.append(
                "CREATE TEMPORARY TABLE TopicsNew (" +
                        "id INT NOT NULL PRIMARY KEY," +
                        "ss INT NOT NULL"
            )
            for (day in tableDaysToSync) {
                createTableQuery.append(",u$day INT,s$day INT")
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
            statement = connection.prepareStatement(
                "INSERT OR REPLACE INTO temp.TopicsNew VALUES (?${",?".repeat(valuesCount * 2)})"
            )
            // эта цыганская магия позволит нам вставлять значения в нужные ячейки и не таскать с собой индексы
            for (topic in topics) {
                statement.setInt(1, topic.topicId)
                for (i in 0 until valuesCount) {
                    statement.setInt(i * 2 + 2, topic.updatesCount[i])
                    statement.setInt(i * 2 + 3, topic.totalSeeds[i])
                }
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
                // FIXME: 17.01.2023 это явно можнет делаться проще и я чего-то не знаю
                insertSyncQuery.append("u$day=excluded.u$day,s$day=excluded.s$day,")
            }
            insertSyncQuery.setCharAt(insertSyncQuery.length - 1, ' ')
            insertSyncQuery.append("FROM (SELECT * FROM temp.TopicsNew WHERE Topics.id = temp.TopicsNew.id")
            insertStatement.addBatch(insertSyncQuery.toString())
            insertStatement.executeBatch()
            connection.commit()
        } finally {
            insertStatement?.close()
            var dropStatement: Statement? = null
            try {
                dropStatement = connection.createStatement()
                dropStatement.execute("DROP TABLE temp.TopicsNew")
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
            statement = connection.createStatement()
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
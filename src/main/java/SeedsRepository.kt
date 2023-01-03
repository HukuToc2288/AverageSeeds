import entities.SeedsInsertItem
import org.sqlite.SQLiteConfig
import org.sqlite.SQLiteConnection
import java.io.Closeable
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Statement

object SeedsRepository {

    val connection = DriverManager.getConnection(
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
            statement.execute(
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

    fun removeUnregisteredTopics(forumId: Int, topics: Collection<Int>) {
        var statement: Statement? = null
        try {
            statement = connection.createStatement()
            statement.execute("DELETE FROM Topics WHERE ss=$forumId AND id NOT IN (${topics.joinToString(",")})")
            connection.commit()
        } finally {
            statement?.close()
        }
    }

    fun setSeedsCount(day: Int, topics: Collection<SeedsInsertItem>) {
        var statement: PreparedStatement? = null
        try {
            statement = connection.prepareStatement(
                "INSERT INTO Topics(id,ss,u$day,s$day) VALUES (?,?,1,?)" +
                        " ON CONFLICT(id) DO UPDATE SET ss=excluded.ss, u$day=1, s$day=excluded.s$day"
            )
            for (topic in topics) {
                statement.setInt(1, topic.topicId)
                statement.setInt(2, topic.forumId)
                statement.setInt(3, topic.seedsCount)
                statement.addBatch()
            }
            statement.execute()
            connection.commit()
        } finally {
            statement?.close()
        }
    }

    fun incrementSeedsCount(day: Int, topics: Collection<SeedsInsertItem>) {
        var statement: PreparedStatement? = null
        try {
            statement = connection.prepareStatement(
                "INSERT INTO Topics(id,ss,u$day,s$day) VALUES (?,?,1,?)" +
                        " ON CONFLICT(id) DO UPDATE SET ss=excluded.ss, u$day=u$day+1, s$day=s$day+excluded.s$day"
            )
            for (topic in topics) {
                statement.setInt(1, topic.topicId)
                statement.setInt(2, topic.forumId)
                statement.setInt(3, topic.seedsCount)
                statement.addBatch()
            }
            statement.execute()
            connection.commit()
        } finally {
            statement?.close()
        }
    }
}
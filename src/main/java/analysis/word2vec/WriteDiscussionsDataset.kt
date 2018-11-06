package analysis.word2vec

import org.jsoup.Jsoup
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.random.Random

val topFolder = File("/media/ehallmark/tank/discussion2vec_data/")

fun main(args: Array<String>) {

    val databases: Array<String> = arrayOf(
            "stackoverflow",
            //"elastico",
            "superuser",
            "serverfault",
            "unix",
            "dba",
            "askubuntu"
    )

    if (!topFolder.exists()) topFolder.mkdir()

    val writers: List<BufferedWriter> = (0 until 1000).map { BufferedWriter(FileWriter(File(topFolder, "file-$it"))) }

    val rand = Random(21352)

    val connections: List<Connection> = databases.map { database ->
        val conn = DriverManager.getConnection("jdbc:postgresql://localhost/$database?user=postgres&password=password&tcpKeepAlive=true")
        conn.autoCommit = false
        conn
    }

    val pses: List<PreparedStatement> = connections.flatMap { conn ->
        val ps1 = conn.prepareStatement("select body from posts")
        val ps2 = conn.prepareStatement("select text from comments")
        ps1.fetchSize = 10
        ps2.fetchSize = 10
        listOf(ps1, ps2)
    }

    var rses: MutableList<ResultSet> = pses.map { it.executeQuery() }.toMutableList()

    var cnt: Long = 0
    while (!rses.isEmpty()) {
        val writer = writers[rand.nextInt(writers.size)]
        val rs = rses[rand.nextInt(rses.size)]

        if(rs.next()) {
            val text = Jsoup.parse(rs.getString(1)).text().replace("\\s+".toRegex(), " ")
            if (text.isNotEmpty()) {
                writer.write(text+"\n")
                if(cnt % 10000 == 9999L) {
                    println("Found $cnt")
                    writer.flush()
                }
                cnt ++
            }

        } else {
            rses.remove(rs)
        }
    }

    for (writer in writers) {
        writer.flush()
        writer.close()
    }

    for (ps in pses) {
        ps.close()
    }

    for (conn in connections) {
        conn.close()
    }
}
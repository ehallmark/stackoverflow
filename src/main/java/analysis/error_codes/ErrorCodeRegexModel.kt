package analysis.error_codes


import database.Database
import javafx.util.Pair
import java.io.*
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*
import java.util.Collections.min
import java.util.stream.Stream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import kotlin.collections.HashMap

const val ERROR_CODE_TO_ANSWER_MAP_FILE = "error_codes_to_answer_map.jobj"
const val POST_TO_DISTINCT_CODES_FILE = "post_to_distinct_num_codes_map.jobj"
const val CORRELATED_ERROR_CODES_FILE = "correlated_error_codes_map.jobj"
const val CRASH_PROBABILITY_MAP_FILE = "crash_probability_map.jobj"
const val targetFatalPercentage = 0.01
const val alpha = 10.0

data class Solution(val errorCode: String, val postId: Int, val score: Int, val views: Int, val answerId: Int, val occurrences: Int) : Serializable

@Throws(IOException::class, ClassNotFoundException::class)
fun loadErrorCodeMap(): Map<String, List<Solution>> {
    val ois = ObjectInputStream(GZIPInputStream(BufferedInputStream(FileInputStream(ERROR_CODE_TO_ANSWER_MAP_FILE))))
    val obj = ois.readObject() as Map<String, List<Solution>>
    ois.close()
    return obj
}

@Throws(IOException::class, ClassNotFoundException::class)
fun loadDistinctCodesPerPostMap(): Map<Int, Int> {
    val ois = ObjectInputStream(GZIPInputStream(BufferedInputStream(FileInputStream(POST_TO_DISTINCT_CODES_FILE))))
    val obj = ois.readObject() as Map<Int, Int>
    ois.close()
    return obj
}

@Throws(IOException::class, ClassNotFoundException::class)
fun loadCrashProbabilities(): Map<String, Double> {
    val ois = ObjectInputStream(GZIPInputStream(BufferedInputStream(FileInputStream(CRASH_PROBABILITY_MAP_FILE))))
    val obj = ois.readObject() as Map<String, Double>
    ois.close()
    return obj
}

@Throws(IOException::class, ClassNotFoundException::class)
fun loadCorrelatedErrors(): Map<String, List<Pair<String, Double>>> {
    val ois = ObjectInputStream(GZIPInputStream(BufferedInputStream(FileInputStream(CORRELATED_ERROR_CODES_FILE))))
    val obj = ois.readObject() as Map<String, List<Pair<String, Double>>>
    ois.close()
    return obj
}

@Throws(IOException::class)
fun writeDataToFile(data: Map<*,*>, filename: String) {
    val oos = ObjectOutputStream(GZIPOutputStream(BufferedOutputStream(FileOutputStream(filename))))
    oos.writeObject(data)
    oos.flush()
    oos.close()
}

fun queryForTable(table: String): String {
    return "select p.id, p.accepted_answer_id, p.score, p.view_count, e.error_code, e.occurrences from posts as p join $table as e on (p.id=e.post_id) where accepted_answer_id is not null and parent_id is null and closed_date is null and score > 0"
}

fun main(args: Array<String>) {
    val test = false
    val conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true")
    conn.autoCommit = false
    val ps = conn.prepareStatement(queryForTable("error_codes")+" union all "+queryForTable("exceptions"))
    ps.fetchSize = 10
    val rs = ps.executeQuery()
    var count = 0

    val data: MutableMap<String, MutableList<Solution>> = HashMap()
    val errorCodesByPost: MutableMap<Int, MutableList<String>> = HashMap()

    while (rs.next()) {
        val postId = rs.getInt(1)
        val answerId = rs.getInt(2)
        val score = rs.getInt(3)
        val views = rs.getInt(4)
        val error: String = rs.getString(5)
        val occurrences = rs.getInt(6)

        val solution = Solution(error, postId, score, views, answerId, occurrences)
        if (error !in data) {
            data[error] = ArrayList()
        }
        if (postId !in errorCodesByPost) {
            errorCodesByPost[postId] = ArrayList()
        }
        errorCodesByPost[postId]?.add(error)
        data[error]?.add(solution)

        if (count % 1000 == 999) {
            println("Count: " + count + ", Num tags: " + data.size)
        }
        if (test && count > 100000) break
        count++
    }
    rs.close()
    ps.close()

    val numDistinctCodesPerPost: Map<Int, Int> = errorCodesByPost.map { kotlin.Pair(it.key, it.value.size) }
            .toMap()

    val correlatedErrorsTmp: MutableMap<String, MutableList<String>> = HashMap()
    errorCodesByPost.values.forEach {
        for (error in it) {
            if (error !in correlatedErrorsTmp) {
                correlatedErrorsTmp[error] = ArrayList()
            }
        }
        for(error in it) {
            for(error2 in it) {
                if (error2 !== error) {
                    correlatedErrorsTmp[error]?.add(error2)
                }
            }
        }
    }



    val correlatedErrors: Map<String, List<Pair<String,Double>>> = correlatedErrorsTmp.map {
        kotlin.Pair(it.key, it.value.groupBy { v -> v }.map { e -> Pair(e.key, (e.value.size.toDouble())/it.value.size) })
    }.toMap()

    val crashCountByPost: MutableMap<Int, Int> = HashMap()
    val ps2 = conn.prepareStatement("select post_id, occurrences from fatal_errors")
    val rs2 = ps2.executeQuery()
    while (rs2.next()) {
        crashCountByPost[rs2.getInt(1)] = rs2.getInt(2)
    }
    rs2.close()
    ps2.close()
    val crashProbabilities: MutableMap<String, kotlin.Pair<Double,Double>> = data.map {
        kotlin.Pair(it.key, kotlin.Pair(alpha* targetFatalPercentage, alpha + it.value.size))
    }.toMap().toMutableMap()

    for ((error, solutions) in data) {
        for (solution in solutions) {
            val postId = solution.postId
            val crashCount: Int? = crashCountByPost[postId]
            if (crashCount != null) {
                val crashProb: kotlin.Pair<Double,Double>? = crashProbabilities[error]
                if (crashProb != null) {
                     crashProbabilities[error] = kotlin.Pair(crashProb.first + crashCount, crashProb.second + crashCount - 1)
                }
            }
        }
    }

    val crashProbabilitiesDouble: MutableMap<String, Double> = crashProbabilities.map {
        kotlin.Pair(it.key, it.value.first/Math.max(1.0, it.value.second))
    }.toMap().toMutableMap()

    writeDataToFile(data, ERROR_CODE_TO_ANSWER_MAP_FILE)
    writeDataToFile(numDistinctCodesPerPost, POST_TO_DISTINCT_CODES_FILE)
    writeDataToFile(correlatedErrors, CORRELATED_ERROR_CODES_FILE)
    writeDataToFile(crashProbabilitiesDouble, CRASH_PROBABILITY_MAP_FILE)

    println("Error code map size: ${data.size}")
    println("numDistinctCodesPerPost size: ${numDistinctCodesPerPost.size}")
    println("correlatedErrors: ${correlatedErrors.size}")
    println("crashProbabilities: ${crashProbabilities.size}")


    conn.close()
}


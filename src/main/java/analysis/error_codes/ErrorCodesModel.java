package analysis.error_codes;

import analysis.min_hash.MinHash;
import analysis.preprocessing.PostsPreprocessor;
import com.opencsv.CSVWriter;
import csv.CSVHelper;
import javafx.util.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ErrorCodesModel {
    public static final String MIN_HASH_FILE = "error_code_min_hash.jobj";
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        boolean test = false;
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);

        PreparedStatement ps = conn.prepareStatement("select body, accepted_answer_id from posts where accepted_answer_id is not null and parent_id is null and closed_date is null and score > 0 and view_count > 100");
        ps.setFetchSize(10);
        ResultSet rs = ps.executeQuery();
        int count = 0;
       // int hasErrors = 0;
       // final String[] errorTerms = new String[]{"error code:", "errorcode:", "trace:", "errcode:", "warning:", "caution:", "notice:", "errno:", "error:", "exception:", "message:", "status:", "response:"};
        MinHash hash = new MinHash(100, -1, 4, 6);

        while(rs.next()) {
            int answerId = rs.getInt(2);
            String text = rs.getString(1).toLowerCase().trim();
            hash.initialize(new Pair<>(String.valueOf(answerId), text));
            if(count%1000==999) {
                System.out.println("Count: "+count);
            }
            if(test && count > 100000) break;
            count++;
        }
        rs.close();
        ps.close();
        conn.close();
        System.out.println("Saving hash...");
        hash.save(MIN_HASH_FILE);
        System.out.println("Saved.");
        hash = MinHash.load(MIN_HASH_FILE);
        System.out.println("Reloaded. "+hash.mostSimilar("illegalstateexception java outofboundserror", 10));
    }
}
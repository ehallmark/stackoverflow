package analysis.error_codes;

import analysis.min_hash.MinHash;
import analysis.preprocessing.PostsPreprocessor;
import com.opencsv.CSVWriter;
import csv.CSVHelper;
import javafx.util.Pair;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ErrorCodesModel {
    public static final String MIN_HASH_FILE = "error_code_min_hash.jobj";
    public static final String TAG_TO_ANSWER_MAP_FILE = "tag_to_answer_map.jobj";
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        boolean test = false;
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        Map<String, List<Integer>> tagsToAnswerIds = new HashMap<>();

        PreparedStatement ps = conn.prepareStatement("select body, accepted_answer_id, tags from posts where accepted_answer_id is not null and parent_id is null and closed_date is null and score > 1 and view_count > 250");
        ps.setFetchSize(10);
        ResultSet rs = ps.executeQuery();
        int count = 0;
       // int hasErrors = 0;
       // final String[] errorTerms = new String[]{"error code:", "errorcode:", "trace:", "errcode:", "warning:", "caution:", "notice:", "errno:", "error:", "exception:", "message:", "status:", "response:"};
        MinHash hash = new MinHash(200, -1, 5, 7);

        while(rs.next()) {
            int answerId = rs.getInt(2);
            String text = rs.getString(1).toLowerCase().trim();
            String tagStr = rs.getString(2).replace("<", "\n").replace(">","");
            for(String tag : tagStr.split("\\n")) {
                tagsToAnswerIds.putIfAbsent(tag, new ArrayList<>());
                tagsToAnswerIds.get(tag).add(answerId);
            }
            hash.initialize(new Pair<>(answerId, text+"\n"+tagStr));
            if(count%1000==999) {
                System.out.println("Count: "+count);
            }
            if(test && count > 100000) break;
            count++;
        }
        rs.close();
        ps.close();
        conn.close();
        writeTagToAnswerMap(tagsToAnswerIds);
        System.out.println("Saving hash...");
        hash.save(MIN_HASH_FILE);
        System.out.println("Saved.");
        tagsToAnswerIds = loadTagToAnswersMap();
        System.out.println("Tag to answer map size: "+tagsToAnswerIds.size());
        hash = MinHash.load(MIN_HASH_FILE);

        System.out.println("Reloaded. "+hash.mostSimilar("illegalstateexception java outofboundserror", 10));
    }


    public static Map<String,List<Integer>> loadTagToAnswersMap() throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(TAG_TO_ANSWER_MAP_FILE))));
        Map<String,List<Integer>> obj = (Map<String,List<Integer>>) ois.readObject();
        ois.close();
        return obj;
    }

    public static void writeTagToAnswerMap(Map<String, List<Integer>> tagToAnswerMap) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(TAG_TO_ANSWER_MAP_FILE))));
        oos.writeObject(tagToAnswerMap);
        oos.flush();
        oos.close();
    }
}
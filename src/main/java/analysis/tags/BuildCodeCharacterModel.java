package analysis.tags;

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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BuildCodeCharacterModel {
    public static final String DATA_FILE = "/media/ehallmark/tank/stack_code_char_tag_prediction_data.csv";
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        boolean test = false;
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(DATA_FILE + (test ? ".test.csv" : "")))));

        PreparedStatement ps = conn.prepareStatement("select coalesce(body,''), coalesce(tags,'') from posts where parent_id is null");
        ps.setFetchSize(10);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        int valid = 0;
        final PostsPreprocessor postsPreprocessor = new PostsPreprocessor();
        Set<String> tags = CSVHelper.readFromCSV("tags_custom.csv").stream().map(s->s[0]).collect(Collectors.toSet());
        long totalCodeLength = 0;
        long totalCodeCount = 0;
        while(rs.next()) {
            String text = postsPreprocessor.getCode(rs.getString(1));

            //System.out.println("Text: "+text);
            //String code = preprocessor.getCode(rs.getString(1));
            final String tagStr = rs.getString(2);
            // write dataset for nn
            final Object[] questionFeatures = postsPreprocessor.getCodeCharFeaturesFor(text, tagStr, tags);
            if (questionFeatures != null && questionFeatures[1].toString().trim().length() > 0) {
                final String[] featuresPos = new String[]{
                        String.join(" ", IntStream.of((int[])questionFeatures[0]).mapToObj(d->String.valueOf(d)).collect(Collectors.toList())),
                        questionFeatures[1].toString()
                };
                totalCodeLength += text.length();
                totalCodeCount ++;
                writer.writeNext(featuresPos);
                valid ++;
            }
            if(count%1000==999) {
                System.out.println("Count: "+count+". Num valid: "+valid+", Avg code length: "+(new Double(totalCodeLength)/totalCodeCount));
                writer.flush();
            }
            if(test && count > 100000) break;
            count++;
        }
        rs.close();
        ps.close();
        conn.close();
        writer.flush();
        writer.close();
    }
}
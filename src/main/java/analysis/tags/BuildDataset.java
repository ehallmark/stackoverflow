package analysis.tags;

import com.opencsv.CSVWriter;
import csv.CSVHelper;
import javafx.util.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BuildDataset {
    public static final String DATA_FILE = "/media/ehallmark/tank/stack_tag_prediction_data.csv";

    private static Object[] getFeaturesFor(ResultSet rs, int startIdx, Map<String,Integer> wordIdxMap) throws SQLException {
        String body = preprocess(rs.getString(startIdx+1),wordIdxMap, 256);
        String tags = rs.getString(startIdx+2);
        String title = preprocess(rs.getString(startIdx+3),wordIdxMap, 32);
        return new Object[]{
                body,
                tags,
                title
        };
    }

    private static String preprocess(String in, Map<String,Integer> vocabIndexMap, int limit) {
        String[] words = in.toLowerCase().replaceAll("[^a-z0-9 ]", " ").split("\\s+");
        StringJoiner sj = new StringJoiner(",");
        int i = 0;
        for(String word : words) {
            Integer idx = vocabIndexMap.get(word);
            if(idx!=null) {
                sj.add(idx.toString());
                i++;
                if(i>=limit) break;
            }
        }
        return sj.toString();
    }

    public static void main(String[] args) throws Exception {
        boolean test = false;
        List<String> vocabulary = CSVHelper.readFromCSV("answers_vocabulary.csv").stream().map(s->s[0]).collect(Collectors.toList());
        Map<String,Integer> vocabIndexMap = IntStream.range(0, vocabulary.size()).mapToObj(i->new Pair<>(vocabulary.get(i), i))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));

        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        // we want to match up questions with:
        //      An answer to the question with a positive score with probability 0.5
        //      A random answer of a random question with probability 0.5

        // start by getting ids
        System.out.println("Starting to read data...");
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(DATA_FILE + (test ? ".test.csv" : "")))));
        PreparedStatement ps = conn.prepareStatement("select coalesce(body,''), coalesce(tags,''), coalesce(title,'') from posts where parent_id is null");
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        int valid = 0;
        while(rs.next()) {
            final Object[] questionFeatures = getFeaturesFor(rs, 0, vocabIndexMap);
            if(questionFeatures[1].toString().trim().length()>0) {
                final String[] featuresPos = new String[]{
                        questionFeatures[0].toString(),
                        questionFeatures[2].toString(),
                        questionFeatures[1].toString()
                };
                writer.writeNext(featuresPos);
                valid++;
            }

            if(count%1000==999) {
                System.out.println("Seen answers: " + count+". Valid: "+valid);
                writer.flush();
                if(test && count > 100000) {
                    writer.close();
                    rs.close();
                    ps.close();
                    conn.close();
                    System.exit(0);
                }
            }
            count++;
        }
        writer.flush();
        writer.close();
        rs.close();
        ps.close();

        conn.close();
        System.out.println("Finished.");
    }
}

package analysis.tags;

import analysis.preprocessing.PostsPreprocessor;
import com.opencsv.CSVWriter;
import csv.CSVHelper;
import javafx.util.Pair;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BuildDataset {
    public static final String DATA_FILE = "/media/ehallmark/tank/stack_tag_prediction_data.csv";


    public static void main(String[] args) throws Exception {
        boolean test = false;
        List<String> vocabulary = CSVHelper.readFromCSV("answers_vocabulary.csv").stream().map(s->s[0]).collect(Collectors.toList());
        Set<String> tags = CSVHelper.readFromCSV("tags1000.csv").stream().map(s->s[0]).collect(Collectors.toSet());
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
        final PostsPreprocessor postsPreprocessor = new PostsPreprocessor();
        while(rs.next()) {
            final Object[] questionFeatures = postsPreprocessor.getFeaturesFor(rs, 0, vocabIndexMap, tags);
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

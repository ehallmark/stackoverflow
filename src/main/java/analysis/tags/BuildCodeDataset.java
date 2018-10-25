package analysis.tags;

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

public class BuildCodeDataset {
    public static final String DATA_FILE = "/media/ehallmark/tank/stack_tag_code_prediction_data.csv";


    public static void main(String[] args) throws Exception {
        boolean test = false;
        List<String> codeVocabulary = CSVHelper.readFromCSV("code_vocabulary.csv").stream().map(s->s[0]).collect(Collectors.toList());
        List<String> questionVocabulary = CSVHelper.readFromCSV("answers_vocabulary.csv").stream().map(s->s[0]).collect(Collectors.toList());
        Set<String> tags = CSVHelper.readFromCSV("tags5000.csv").stream().map(s->s[0]).collect(Collectors.toSet());
        Map<String,Integer> codeVocabIndexMap = IntStream.range(0, codeVocabulary.size()).mapToObj(i->new Pair<>(codeVocabulary.get(i), i))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));
        Map<String,Integer> answerVocabIndexMap = IntStream.range(0, questionVocabulary.size()).mapToObj(i->new Pair<>(questionVocabulary.get(i), i))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));

        List<Integer> posts = new LinkedList<>();

        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        // we want to match up questions with:
        //      An answer to the question with a positive score with probability 0.5
        //      A random answer of a random question with probability 0.5

        // start by getting ids
        System.out.println("Starting to read data...");
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(DATA_FILE + (test ? ".test.csv" : "")))));
        PreparedStatement ps = conn.prepareStatement("select coalesce(body,''), coalesce(tags,''), id, coalesce(parent_id,0), coalesce(accepted_answer_id,0) from posts where parent_id is null");
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        int valid = 0;
        final PostsPreprocessor postsPreprocessor = new PostsPreprocessor();
        while(rs.next()) {
            final String[] questionFeatures = postsPreprocessor.getAllFeaturesFor(rs, 0, answerVocabIndexMap, codeVocabIndexMap, tags);
            if(questionFeatures!=null && questionFeatures[4].trim().length()>0) {
                writer.writeNext(questionFeatures);
                posts.add(rs.getInt(3));
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
        System.out.println("Sorting post ids... (n="+posts.size()+")");
        Collections.sort(posts);
        CSVHelper.writeToCSV("all_post_ids.csv", posts.stream().map(id->new String[]{String.valueOf(id)}).collect(Collectors.toList()));
        System.out.println("Finished.");
    }
}

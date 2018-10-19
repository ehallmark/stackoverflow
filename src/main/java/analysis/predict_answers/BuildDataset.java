package analysis.predict_answers;

import analysis.preprocessing.PostsPreprocessor;
import com.opencsv.CSVWriter;
import csv.CSVHelper;
import javafx.util.Pair;
import org.eclipse.jetty.util.ArrayUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BuildDataset {
    public static final String DATA_FILE = "/media/ehallmark/tank/stack_answer_prediction_data.csv";

    public static void main(String[] args) throws Exception {
        boolean test = false;
        List<String> vocabulary = CSVHelper.readFromCSV("answers_vocabulary.csv").stream().map(s->s[0]).collect(Collectors.toList());
        Map<String,Integer> vocabIndexMap = IntStream.range(0, vocabulary.size()).mapToObj(i->new Pair<>(vocabulary.get(i), i))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));
        final Set<String> tags = CSVHelper.readFromCSV("tags5000.csv").stream().map(s->s[0]).collect(Collectors.toSet());

        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        final Connection conn2 = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);
        // we want to match up questions with:
        //      An answer to the question with a positive score with probability 0.5
        //      A random answer of a random question with probability 0.5

        // start by getting ids
        System.out.println("Starting to read data...");
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(DATA_FILE + (test ? ".test.csv" : "")))));
        PreparedStatement ps = conn.prepareStatement("select coalesce(parent_body,''), coalesce(parent_tags,''), coalesce(parent_title,''), coalesce(body,''), coalesce(tags,''), coalesce(title,'') from answers_with_question order by id");
        PreparedStatement ps2 = conn2.prepareStatement("select coalesce(parent_body,''), coalesce(parent_tags,''), coalesce(parent_title,''), coalesce(body,''), coalesce(tags,''), coalesce(title,'') from answers_with_question order by random()");
        ps.setFetchSize(100);
        ps2.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        ResultSet rs2 = ps2.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        final PostsPreprocessor postsPreprocessor = new PostsPreprocessor();

        while(rs.next() && rs2.next()) {
            final Object[] questionFeatures = postsPreprocessor.getFeaturesFor(rs, 0, vocabIndexMap, tags);
            final Object[] actualAnswerFeatures = postsPreprocessor.getFeaturesFor(rs, 3, vocabIndexMap, tags);
            final Object[] randomQuestionFeatures = postsPreprocessor.getFeaturesFor(rs2, 0, vocabIndexMap, tags);
            final Object[] randomAnswerFeatures = postsPreprocessor.getFeaturesFor(rs2, 3, vocabIndexMap, tags);

            final String[] featuresPos = new String[]{
                    questionFeatures[0].toString(),
                    questionFeatures[1].toString(),
                    questionFeatures[2].toString(),
                    actualAnswerFeatures[0].toString(),
                    "1",
            };

            writer.writeNext(featuresPos);

            final String[] featuresNeg = new String[]{
                    questionFeatures[0].toString(), // keep true question body
                    randomQuestionFeatures[1].toString(), // use random other features
                    randomQuestionFeatures[2].toString(),
                    randomAnswerFeatures[0].toString(),
                    "0"
            };

            writer.writeNext(featuresNeg);
            if(count%1000==999) {
                System.out.println("Seen answers: " + count);
                writer.flush();
                if(test && count > 100000) {
                    writer.close();
                    rs2.close();
                    ps2.close();
                    rs.close();
                    ps.close();
                    conn.close();
                    conn2.close();
                    System.exit(0);
                }
            }
            count++;
        }
        writer.flush();
        writer.close();
        rs.close();
        ps.close();
        rs2.close();
        ps2.close();

        conn.close();
        conn2.close();

        System.out.println("Finished.");
    }
}

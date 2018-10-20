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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class BuildCodeDataset {
    public static final String DATA_FILE = "/media/ehallmark/tank/stack_tag_code_prediction_data.csv";


    public static void main(String[] args) throws Exception {
        boolean test = false;
        List<String> vocabulary = CSVHelper.readFromCSV("code_vocabulary.csv").stream().map(s->s[0]).collect(Collectors.toList());
        Set<String> tags = CSVHelper.readFromCSV("tags_custom.csv").stream().map(s->s[0]).collect(Collectors.toSet());
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
        PreparedStatement ps = conn.prepareStatement("select coalesce(body,''), coalesce(tags,'') from posts where parent_id is null");
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        int valid = 0;
        final PostsPreprocessor postsPreprocessor = new PostsPreprocessor();
        while(rs.next()) {
            final Object[] questionFeatures = postsPreprocessor.getCodeFeaturesFor(rs, 0, vocabIndexMap, tags);
            if(questionFeatures!=null && questionFeatures[2].toString().trim().length()>0) {
                final String[] featuresPos = new String[]{
                        questionFeatures[0].toString(),
                        String.join(" ", DoubleStream.of((double[])questionFeatures[1]).mapToObj(d->String.valueOf(d)).collect(Collectors.toList())),
                        questionFeatures[2].toString()
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

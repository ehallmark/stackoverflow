package analysis.tags;

import analysis.preprocessing.PostsPreprocessor;
import analysis.word2vec.DiscussionsToVec;
import com.opencsv.CSVWriter;
import csv.CSVHelper;
import javafx.util.Pair;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BuildStack2VecDataset {
    public static final String DATA_FILE = "/media/ehallmark/tank/stack2vec_rnn_data.csv";


    public static void main(String[] args) throws Exception {
        boolean test = false;
        final Set<String> tags = CSVHelper.readFromCSV("tags5000.csv").stream().map(s->s[0]).collect(Collectors.toSet());
        final Map<String, Integer> tagCounts = CSVHelper.readFromCSV("tags_count_map.csv").stream().map(s->new Pair<>(s[0], Integer.valueOf(s[1])))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));
        final int maxTimeSteps = 512;
        Word2Vec word2Vec = DiscussionsToVec.load256Model();
        if(word2Vec == null) {
            throw new RuntimeException("Unable to load word2vec.");
        }

        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);

        // start by getting ids
        System.out.println("Starting to read data...");
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(DATA_FILE + (test ? ".test.csv" : "")))));

        PreparedStatement ps = conn.prepareStatement("select body, tags from posts where tags is not null and char_length(tags) > 0 and body is not null and parent_id is null");
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        int valid = 0;
        final TokenizerFactory tokenizerFactory = new DefaultTokenizerFactory();
        tokenizerFactory.setTokenPreProcessor(new CommonPreprocessor());

        while(rs.next()) {
            final String body = rs.getString(1);
            String label = Stream.of(rs.getString(2).split("><"))
                    .map(s->s.replace("<","").replace(">",""))
                    .filter(s->s.length()>0 && tags.contains(s))
                    .min(Comparator.comparingInt(e->tagCounts.getOrDefault(e, 0)))
                    .orElse(null);

            if(label!=null) {
                String[] features = new String[maxTimeSteps + 1];
                List<String> tokens = tokenizerFactory.create(body.toLowerCase()).getTokens();
                for(int i = 0; i < Math.min(tokens.size(), maxTimeSteps); i++) {
                    if (tokens.size() > i) {
                        features[i] = word2Vec.hasWord(tokens.get(i)) ? String.valueOf((word2Vec.indexOf(tokens.get(i)) + 1)) : "0";
                    } else {
                        features[i] = "0";
                    }
                }
                features[maxTimeSteps] = label;

                writer.writeNext(features);
                valid++;
            }

            if (count % 1000 == 999) {
                System.out.println("Seen answers: " + count + ". Valid: " + valid);
                writer.flush();
                if (test && count > 100000) {
                    break;
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

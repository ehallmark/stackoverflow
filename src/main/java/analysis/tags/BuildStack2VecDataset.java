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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BuildStack2VecDataset {
    public static void main(String[] args) throws Exception {
        boolean test = false;
        final List<String> tagList = CSVHelper.readFromCSV("tags1000.csv").stream().map(s->s[0]).collect(Collectors.toList());
        Map<String,Integer> tagIndexMap = IntStream.range(0, tagList.size()).mapToObj(i->new Pair<>(i, tagList.get(i)))
                .collect(Collectors.toMap(e->e.getValue(), e->e.getKey()));
        final Set<String> tags = new HashSet<>(tagList);
        System.out.println("Num tags: "+tags.size());
        final Map<String, Integer> tagCounts = CSVHelper.readFromCSV("tags_count_map.csv").stream().map(s->new Pair<>(s[0], Integer.valueOf(s[1])))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));
        final int numFiles = 1;
        final int maxTimeSteps = 256;
        Word2Vec word2Vec = DiscussionsToVec.load256Model();
        if(word2Vec == null) {
            throw new RuntimeException("Unable to load word2vec.");
        }

        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);

        // start by getting ids
        System.out.println("Starting to read data...");

        PreparedStatement ps = conn.prepareStatement("select body, tags from posts where tags is not null and char_length(tags) > 0 and body is not null and parent_id is null");
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        int valid = 0;
        final TokenizerFactory tokenizerFactory = new DefaultTokenizerFactory();
        tokenizerFactory.setTokenPreProcessor(new CommonPreprocessor());
        final Random rand = new Random(1251);
        File folder = new File("/media/ehallmark/tank/stack2vec_rnn_data_sampling256_single");
        folder.mkdir();

        CSVWriter[] writers = new CSVWriter[numFiles];
        for(int i = 0; i < writers.length; i++) {
            writers[i] = new CSVWriter(new BufferedWriter(new FileWriter(new File(folder, String.valueOf(i)+".csv"))));
        }


        while(rs.next()) {
            final String body = rs.getString(1);
            List<Pair<String,Integer>> labelPairs = Stream.of(rs.getString(2).split("><"))
                    .map(s->s.replace("<","").replace(">",""))
                    .filter(s->s.length()>0 && tags.contains(s))
                    .map(s->new Pair<>(s, tagCounts.getOrDefault(s, 0)+10))
                    .collect(Collectors.toList());
            if(labelPairs.size()>0) {
                double[] probabilities = labelPairs.stream().mapToDouble(p -> 1.0/p.getValue()).toArray();
                double sum = DoubleStream.of(probabilities).sum();
                for(int i = 0; i < probabilities.length; i++) {
                    probabilities[i] /= sum;
                }
                double r = rand.nextDouble();
                double v = 0d;
                Integer label = null;
                for(int i = 0; i < probabilities.length; i++) {
                    v += probabilities[i];
                    if (v >= r) {
                        label = tagIndexMap.get(labelPairs.get(i).getKey());
                        break;
                    }
                }

                if (label != null) {
                    String[] features = new String[maxTimeSteps + 1];
                    List<String> tokens = tokenizerFactory.create(body.toLowerCase()).getTokens();
                    for (int i = 0; i < maxTimeSteps; i++) {
                        if (tokens.size() > i) {
                            features[i] = word2Vec.hasWord(tokens.get(i)) ? String.valueOf((word2Vec.indexOf(tokens.get(i)) + 1)) : "0";
                        } else {
                            features[i] = "0";
                        }
                    }
                    features[maxTimeSteps] = label.toString();

                    writers[valid % numFiles].writeNext(features, false);
                    valid++;
                }
            }

            if (count % 1000 == 999) {
                System.out.println("Seen answers: " + count + ". Valid: " + valid);
                for(CSVWriter writer : writers) {
                    writer.flush();
                }
                if (test && count > 100000) {
                    break;
                }
            }
            count++;
        }
        for(CSVWriter writer : writers) {
            writer.flush();
            writer.close();
        }


        rs.close();
        ps.close();
        conn.close();

        System.out.println("Finished.");
    }
}

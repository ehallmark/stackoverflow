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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BuildCodeCharacterModel {
    public static final String DATA_FILE = "/media/ehallmark/tank/stack_code_char_tag_prediction_data_1000.csv";
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        boolean test = false;
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(DATA_FILE + (test ? ".test.csv" : "")))));

        //final List<String> tagList = CSVHelper.readFromCSV("tags_custom.csv").stream().map(s->s[0]).collect(Collectors.toList());
        final List<String> tagList = CSVHelper.readFromCSV("tags1000.csv").stream().map(s->s[0]).collect(Collectors.toList());
        Map<String,Integer> tagIndexMap = IntStream.range(0, tagList.size()).mapToObj(i->new Pair<>(i, tagList.get(i)))
                .collect(Collectors.toMap(e->e.getValue(), e->e.getKey()));
        final Set<String> tags = new HashSet<>(tagList);
        System.out.println("Num tags: "+tags.size());
        final Map<String, Integer> tagCounts = CSVHelper.readFromCSV("tags_count_map.csv").stream().map(s->new Pair<>(s[0], Integer.valueOf(s[1])))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));


        PreparedStatement ps = conn.prepareStatement("select body, tags from posts where tags is not null and char_length(tags) > 0 and body is not null and parent_id is null");
        ps.setFetchSize(10);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        int valid = 0;
        final PostsPreprocessor postsPreprocessor = new PostsPreprocessor();
        long totalCodeLength = 0;
        long totalCodeCount = 0;
        final Random rand = new Random(235121);
        final int maxTimeSteps = 1024;
        while(rs.next()) {
            String code = postsPreprocessor.getCode(rs.getString(1));
            totalCodeCount ++;
            totalCodeLength += code.length();
            int[] charFeatures = postsPreprocessor.getCharsAsTimeSeries(code, maxTimeSteps);

            List<Pair<String,Integer>> labelPairs = Stream.of(rs.getString(2).split("><"))
                    .map(s->s.replace("<","").replace(">",""))
                    .filter(s->s.length()>0 && tags.contains(s))
                    .map(s->new Pair<>(s, tagCounts.getOrDefault(s, 0)+10))
                    .collect(Collectors.toList());


            if(labelPairs.size()>0 && code.length() > 5) {
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
                    for (int i = 0; i < maxTimeSteps; i++) {
                        if (charFeatures.length > i) {
                            features[i] = String.valueOf(charFeatures[i]);
                        } else {
                            features[i] = "0";
                        }
                    }
                    features[maxTimeSteps] = label.toString();

                    writer.writeNext(features, false);
                    valid++;
                }
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
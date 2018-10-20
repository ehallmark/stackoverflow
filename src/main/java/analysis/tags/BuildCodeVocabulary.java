package analysis.tags;

import analysis.preprocessing.PostsPreprocessor;
import csv.CSVHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

public class BuildCodeVocabulary {
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);

        int minVocabularySize = 30;

        PreparedStatement ps = conn.prepareStatement("select body from posts tablesample system (25)");
        ps.setFetchSize(10);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        Map<String,Integer> wordCountMap = new HashMap<>();
        final PostsPreprocessor preprocessor = new PostsPreprocessor();
        while(rs.next()) {
            String text = rs.getString(1);
            String[] words = preprocessor.getCode(text).toLowerCase().replaceAll("[^a-z0-9 ]", " ").split("\\s+");
            for(String word : words) {
                if(word.length()>0) {
                    wordCountMap.putIfAbsent(word, 0);
                    wordCountMap.put(word, wordCountMap.get(word) + 1);
                }
            }
            if(count%100==99) {
                System.out.println("Count: "+count+". Num distinct words: "+wordCountMap.size());
            }
            count++;
        }
        rs.close();
        ps.close();
        conn.close();

        System.out.println("Vocab size before truncation: "+wordCountMap.size());

        wordCountMap = wordCountMap.entrySet().stream().filter(e->e.getValue()>=minVocabularySize)
                .collect(Collectors.toMap(e->e.getKey(),e->e.getValue()));

        System.out.println("Vocab size: "+wordCountMap.size());
        List<String> vocabulary = new ArrayList<>(wordCountMap.keySet());
        Collections.sort(vocabulary);

        String vocabFile = "code_vocabulary.csv";

        String vocabCountFile = "code_vocabulary_count_map.csv";

        CSVHelper.writeToCSV(vocabFile, vocabulary.stream().map(e->new String[]{e}).collect(Collectors.toList()));
        CSVHelper.writeToCSV(vocabCountFile, wordCountMap.entrySet().stream().map(e->new String[]{e.getKey(), e.getValue().toString()}).collect(Collectors.toList()));
    }



}
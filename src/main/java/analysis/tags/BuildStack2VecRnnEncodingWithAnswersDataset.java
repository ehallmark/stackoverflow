package analysis.tags;

import analysis.predict_answers.ReadPostEmbeddings;
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
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BuildStack2VecRnnEncodingWithAnswersDataset {

    public static void main(String[] args) throws Exception {
        boolean test = false;
        final int embeddingSize = 256;

        Pair<List<Integer>, List<float[]>> embeddingData = ReadPostEmbeddings.loadData();
        final List<float[]> embeddings = embeddingData.getValue();
        final List<Integer> postIds = embeddingData.getKey();
        final Map<Integer,Integer> postIdToIdxMap = IntStream.range(0, postIds.size()).boxed()
                .collect(Collectors.toMap(i->postIds.get(i), i->i));

        // start by getting ids
        System.out.println("Starting to read data...");

        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("select parent_id, id from answers_with_question");
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        int valid = 0;

        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File("/media/ehallmark/tank/stack2vec_rnn_encoding_with_answer_data.csv"))));

        while(rs.next()) {
            final int questionId = rs.getInt(1);
            final int answerId = rs.getInt(2);

            final Integer questionIdx = postIdToIdxMap.get(questionId);
            final Integer answerIdx = postIdToIdxMap.get(answerId);

            if(questionIdx!=null && answerIdx!=null) {

                String[] data = new String[embeddingSize*2];

                float[] featureData = embeddings.get(questionIdx);
                float[] targetData = embeddings.get(answerIdx);

                for (int i = 0; i < embeddingSize; i++) {
                    data[i] = String.valueOf(featureData[i]);
                    data[i+embeddingSize] = String.valueOf(targetData[i]);
                }
                writer.writeNext(data, false);
                valid++;
            }

            if (count % 1000 == 999) {
                System.out.println("Seen: " + count + ". Valid: " + valid);
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

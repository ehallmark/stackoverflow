package analysis.tags;

import analysis.word2vec.DiscussionsToVec;
import com.opencsv.CSVWriter;
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
import java.util.List;
import java.util.Map;

public class BuildStack2VecRnnEncodingCodeDataset {

    public static void main(String[] args) throws Exception {
        boolean test = false;
        boolean useAnswers = true;

        final int maxTimeSteps = 256;
        final Map<String,Integer> word2Vec = DiscussionsToVec.loadWordToIndexMap();
        if(word2Vec == null) {
            throw new RuntimeException("Unable to load word2vec.");
        }

        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        // we want to match up questions with:
        //      An answer to the question with a positive score with probability 0.5
        //      A random answer of a random question with probability 0.5

        // start by getting ids
        System.out.println("Starting to read data...");

        String sql = "select body from posts where body is not null";
        if(!useAnswers) {
            sql += " and parent_id is null";
        }
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        int valid = 0;
        final TokenizerFactory tokenizerFactory = new DefaultTokenizerFactory();
        tokenizerFactory.setTokenPreProcessor(new CommonPreprocessor());

        String filename = "/media/ehallmark/tank/stack2vec_rnn_encoding_data";
        if(useAnswers) {
            filename += "_with_answers.csv";
        } else {
            filename += ".csv";
        }

        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(filename))));

        while(rs.next()) {
            final String question = rs.getString(1);
            final int label = 1;

            if(question.length() > 0) {
                String[] features = new String[maxTimeSteps + 1];
                List<String> questionTokens = tokenizerFactory.create(question.toLowerCase()).getTokens();
                if(questionTokens.size()>5) {
                    for (int i = 0; i < maxTimeSteps; i++) {
                        if (questionTokens.size() > i) {
                            features[i] = word2Vec.containsKey(questionTokens.get(i)) ? String.valueOf((word2Vec.get(questionTokens.get(i)) + 1)) : "0";
                        } else {
                            features[i] = "0";
                        }
                    }
                    features[maxTimeSteps] = String.valueOf(label);
                    writer.writeNext(features, false);
                    valid++;
                }
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

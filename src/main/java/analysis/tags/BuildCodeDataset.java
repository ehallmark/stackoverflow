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
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BuildCodeDataset {

    public static void main(String[] args) throws Exception {
        boolean test = false;

        final int maxTimeSteps = 256;
        final Word2Vec word2Vec = DiscussionsToVec.load256Model();
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

        PreparedStatement ps1 = conn.prepareStatement("select body, parent_body from answers_with_question order by random()");
        PreparedStatement ps2 = conn.prepareStatement("select body, parent_body from answers_with_question order by random()");
        ps1.setFetchSize(100);
        ps2.setFetchSize(100);
        ResultSet rs1 = ps1.executeQuery();
        ResultSet rs2 = ps2.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        int valid = 0;
        final TokenizerFactory tokenizerFactory = new DefaultTokenizerFactory();
        tokenizerFactory.setTokenPreProcessor(new CommonPreprocessor());
        final Random rand = new Random(1251);

        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File("/media/ehallmark/tank/stack2vec_rnn_encoding_data.csv"))));

        final double falseProb = 0.8;
        while(rs1.next() && rs2.next()) {
            final String question = rs1.getString(2);
            final String answer;
            final int label;
            if(rand.nextDouble() < falseProb) {
                // get false
                answer = rs2.getString(1);
                label = 0;
            } else {
                answer = rs1.getString(1);
                label = 1;
            }

            if(question.length() > 0 && answer.length() > 0) {
                String[] features = new String[maxTimeSteps * 2 + 1];
                List<String> questionTokens = tokenizerFactory.create(question.toLowerCase()).getTokens();
                List<String> answerTokens = tokenizerFactory.create(answer.toLowerCase()).getTokens();
                if(questionTokens.size()>5 && answerTokens.size()>5) {
                    for (int i = 0; i < maxTimeSteps; i++) {
                        if (questionTokens.size() > i) {
                            features[i] = word2Vec.hasWord(questionTokens.get(i)) ? String.valueOf((word2Vec.indexOf(questionTokens.get(i)) + 1)) : "0";
                        } else {
                            features[i] = "0";
                        }
                        if(answerTokens.size() > i) {
                            features[i + maxTimeSteps] = word2Vec.hasWord(answerTokens.get(i)) ? String.valueOf((word2Vec.indexOf(answerTokens.get(i)) + 1)) : "0";
                        } else {
                            features[i + maxTimeSteps] = "0";
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

        rs1.close();
        ps1.close();
        rs2.close();
        ps2.close();
        conn.close();

        System.out.println("Finished.");
    }
}

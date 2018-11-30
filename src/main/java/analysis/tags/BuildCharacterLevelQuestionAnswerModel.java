package analysis.tags;

import analysis.preprocessing.PostsPreprocessor;
import com.opencsv.CSVWriter;
import csv.CSVHelper;
import javafx.util.Pair;
import org.jsoup.Jsoup;

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

public class BuildCharacterLevelQuestionAnswerModel {
    public static final String DATA_FILE = "/media/ehallmark/tank/stack_code_char_question_answer_data.csv";
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        boolean test = false;
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(DATA_FILE + (test ? ".test.csv" : "")))));

        PreparedStatement ps1 = conn.prepareStatement("select body, tags from posts where parent_id is null order by random() limit 5000000");
        PreparedStatement ps2 = conn.prepareStatement("select body, tags from posts where parent_id is not null order by random() limit 5000000");
        ps1.setFetchSize(10);
        ps2.setFetchSize(10);
        ResultSet rs1 = ps1.executeQuery();
        ResultSet rs2 = ps2.executeQuery();
        int count = 0;
        int valid = 0;
        final PostsPreprocessor postsPreprocessor = new PostsPreprocessor();
        long totalCodeLength = 0;
        long totalCodeCount = 0;
        final int maxTimeSteps = 1024;
        String[] features = new String[maxTimeSteps + 1];
        while(rs1.next() && rs2.next()) {
            String question = Jsoup.parse(rs1.getString(1)).text();
            String answer = Jsoup.parse(rs2.getString(1)).text();
            totalCodeLength += (question.length()+answer.length())/2;
            totalCodeCount ++;
            int[] charFeatures1 = postsPreprocessor.getCharsAsTimeSeries(question, maxTimeSteps);
            int[] charFeatures2 = postsPreprocessor.getCharsAsTimeSeries(answer, maxTimeSteps);

            if(question.length() > 5 && answer.length() > 5) {
                for (int i = 0; i < maxTimeSteps; i++) {
                    if (charFeatures1.length > i) {
                        features[i] = String.valueOf(charFeatures1[i]);
                    } else {
                        features[i] = "0";
                    }
                }
                features[maxTimeSteps] = "0";
                writer.writeNext(features, false);
                for (int i = 0; i < maxTimeSteps; i++) {
                    if (charFeatures2.length > i) {
                        features[i] = String.valueOf(charFeatures2[i]);
                    } else {
                        features[i] = "0";
                    }
                }
                features[maxTimeSteps] = "1";
                writer.writeNext(features, false);
                valid++;
            }
            if(count%1000==999) {
                System.out.println("Count: "+count+". Num valid: "+valid+", Avg code length: "+(new Double(totalCodeLength)/totalCodeCount));
                writer.flush();
            }
            if(test && count > 100000) break;
            count++;
        }
        rs1.close();
        ps1.close();
        rs2.close();
        ps2.close();
        conn.close();
        writer.flush();
        writer.close();
    }
}
package analysis.predict_answers;

import com.opencsv.CSVWriter;
import org.eclipse.jetty.util.ArrayUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BuildDataset {

    private static Object[] getFeaturesFor(ResultSet rs, int startIdx) throws SQLException {
        String body = rs.getString(startIdx+1);
        String tags = rs.getString(startIdx+2);
        String title = rs.getString(startIdx+3);
        return new Object[]{
                body,
                tags,
                title
        };
    }

    public static void main(String[] args) throws Exception {
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        final Connection conn2 = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);
        // we want to match up questions with:
        //      An answer to the question with a positive score with probability 0.5
        //      A random answer of a random question with probability 0.5

        // start by getting ids
        System.out.println("Starting to read data...");
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File("answer_prediction_data.csv"))));
        PreparedStatement ps = conn.prepareStatement("select coalesce(parent_body,''), coalesce(parent_tags,''), coalesce(parent_title,''), coalesce(body,''), coalesce(tags,''), coalesce(title,'') from answers_with_question order by id");
        PreparedStatement ps2 = conn2.prepareStatement("select coalesce(body,''), coalesce(tags,''), coalesce(title,'') from answers_with_question order by random()");
        ps.setFetchSize(100);
        ps2.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        ResultSet rs2 = ps.executeQuery();
        int count = 0;
        System.out.println("Iterating...");
        while(rs.next() && rs2.next()) {
            final Object[] questionFeatures = getFeaturesFor(rs, 0);
            final Object[] actualAnswerFeatures = getFeaturesFor(rs, 4);
            final Object[] randomAnswerFeatures = getFeaturesFor(rs2, 0);

            final String[] featuresPos = new String[]{
                    questionFeatures[0].toString(),
                    questionFeatures[1].toString(),
                    questionFeatures[2].toString(),
                    actualAnswerFeatures[0].toString(),
                    "1",
            };

            writer.writeNext(featuresPos);

            final String[] featuresNeg = new String[]{
                    questionFeatures[0].toString(),
                    questionFeatures[1].toString(),
                    questionFeatures[2].toString(),
                    randomAnswerFeatures[0].toString(),
                    "0"
            };

            writer.writeNext(featuresNeg);
            if(count%1000==999) {
                System.out.println("Seen answers: " + count);
                writer.flush();
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

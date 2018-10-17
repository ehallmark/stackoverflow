package analysis.predict_answers;

import org.eclipse.jetty.util.ArrayUtil;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BuildDataset {

    private static Object[] getFeaturesFor(ResultSet rs, int startIdx) throws SQLException {
        String body = rs.getString(startIdx+1);
        String tagsStr = rs.getString(startIdx+2);
        String[] tags = tagsStr==null||tagsStr==null?new String[]{} : Stream.of(tagsStr.split("><"))
                .map(str->str.replace("<","").replace(">","")).toArray(size->new String[size]);
        tags = ArrayUtil.removeFromArray(tags, "");
        String title = rs.getString(startIdx+3);
        return new Object[]{
                body,
                tags,
                title
        };
    }

    public static void main(String[] args) throws Exception {
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        // we want to match up questions with:
        //      An answer to the question with a positive score with probability 0.5
        //      A random answer of a random question with probability 0.5

        // start by getting ids
        PreparedStatement ps = conn.prepareStatement("select parent_body, parent_tags, parent_title, body, tags, title from posts order by id");
        PreparedStatement ps2 = conn.prepareStatement("select body, tags, title from answers_with_question order by random()");
        ps.setFetchSize(100);
        ps2.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        ResultSet rs2 = ps.executeQuery();
        final List<Object> features = new ArrayList<>();
        final List<Object> labels = new ArrayList<>();
        int count = 0;
        while(rs.next() && rs2.next()) {
            final Object[] questionFeatures = getFeaturesFor(rs, 0);
            final Object[] actualAnswerFeatures = getFeaturesFor(rs, 4);
            final Object[] randomAnswerFeatures = getFeaturesFor(rs2, 0);

            // actual
            labels.add(1);
            features.add(new Object[]{questionFeatures, actualAnswerFeatures});

            // random
            labels.add(0);
            features.add(new Object[]{questionFeatures, randomAnswerFeatures});

            if(count%1000==999) {
                System.out.println("Seen answers: " + count);
            }
            count++;
        }
        rs.close();
        ps.close();
        rs2.close();
        ps2.close();

        System.out.println("Features size: "+features.size());
        System.out.println("Labels size: "+labels.size());


        conn.close();
    }
}

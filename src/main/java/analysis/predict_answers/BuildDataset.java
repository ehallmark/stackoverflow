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
        final List<Integer> allQuestionIds = new ArrayList<>();
        final List<Integer> allAnswerIds = new ArrayList<>();
        final Map<Integer,Integer> answerIdToQuestionIdMap = new HashMap<>();
        PreparedStatement ps = conn.prepareStatement("select id,parent_id,score from posts");
        ps.setFetchSize(1000);
        ResultSet rs = ps.executeQuery();
        while(rs.next()) {
            int id = rs.getInt(1);
            Integer parentId = (Integer) rs.getObject(2);
            if(parentId==null) {
                // question
                allQuestionIds.add(id);
            } else {
                allAnswerIds.add(id);
                Integer score = (Integer)rs.getObject(3);
                if(score==null||score>=0) {
                    answerIdToQuestionIdMap.put(id, parentId);
                }
            }
        }
        rs.close();
        ps.close();
        final Map<Integer,List<Integer>> questionIdToAnswerIdsMap = answerIdToQuestionIdMap.entrySet()
                .stream().collect(Collectors.groupingBy(e->e.getValue(),Collectors.mapping(e->e.getKey(), Collectors.toList())));

        System.out.println("QuestionID Map size: "+questionIdToAnswerIdsMap.size());

        final Random random = new Random(235211);

        final List<Object> features = new ArrayList<>();
        final List<Object> labels = new ArrayList<>();
        final Map<Integer, Object[]> answerData = new HashMap<>();
        PreparedStatement ps2 = conn.prepareStatement("select id, body, tags, title from posts where parent_id is not null");
        ps2.setFetchSize(1000);
        ResultSet rs2 = ps2.executeQuery();
        {
            int count = 0;
            while (rs2.next()) {
                int id = rs2.getInt(1);
                Object[] answerFeatures = getFeaturesFor(rs2, 1);
                answerData.put(id, answerFeatures);
                if(count%1000==999) {
                    System.out.println("Seen answers: " + count);
                }
                count++;
            }
            rs2.close();
            ps2.close();
        }

        // build dataset
        ps = conn.prepareStatement("select id, body, tags, title from posts where parent_id is null");

        ps.setFetchSize(50);
        rs = ps.executeQuery();
        int count = 0;
        while(rs.next()) {
            int questionId = rs.getInt(1);
            List<Integer> answerIds = questionIdToAnswerIdsMap.get(questionId);
            if(answerIds==null) continue;

            final Object[] questionFeatures = getFeaturesFor(rs, 1);
            for (int i = 0; i < answerIds.size(); i++) {
                // add answer
                {
                    int answerId = answerIds.get(i);
                    features.add(new Object[]{questionFeatures, answerData.get(answerId)});
                    labels.add(1);
                }

                // add random (if found data for non-random)
                {
                    int randomAnswerId = allAnswerIds.get(random.nextInt(allAnswerIds.size()));
                    features.add(new Object[]{questionFeatures, answerData.get(randomAnswerId)});
                    labels.add(0);
                }
                if(count % 1000 == 999) {
                    System.out.println("Seen: "+count);
                }
                count++;
            }

        }
        ps2.close();
        rs.close();
        ps.close();
        System.out.println("Features size: "+features.size());
        System.out.println("Labels size: "+labels.size());


        conn.close();
    }
}

package analysis.predict_answers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

public class BuildDataset {
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
        PreparedStatement ps = conn.prepareStatement("select id,parent_id,score from posts limit 10000000");
        ps.setFetchSize(100);
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
                if(score!=null&&score>0) {
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

        // build dataset
        questionIdToAnswerIdsMap.forEach((questionId, answerIds)->{
            Object questionFeatures = null;
            for(int i = 0; i < answerIds.size(); i++) {
                // add answer
                {
                    int answerId = answerIds.get(i);
                    // TODO get features
                    Object answerFeatures = null;
                    features.add(new Object[]{questionFeatures, answerFeatures});
                    labels.add(1);
                }

                // add random
                {
                    int randomAnswerId = allAnswerIds.get(random.nextInt(allAnswerIds.size()));
                    // TODO get features
                    Object answerFeatures = null;
                    features.add(new Object[]{questionFeatures, answerFeatures});
                    labels.add(0);
                }
            }
        });

        System.out.println("Features size: "+features.size());
        System.out.println("Labels size: "+labels.size());
    }
}

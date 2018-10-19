package analysis.error_codes;

import analysis.preprocessing.PostsPreprocessor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ErrorCodesModel {
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);

        PreparedStatement ps = conn.prepareStatement("select body from posts where parent_id is null");
        ps.setFetchSize(10);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        int hasErrors = 0;
        final String[] errorTerms = new String[]{"error:", "errno:", "exception:", "err:", "code:", "status:"};
        final PostsPreprocessor preprocessor = new PostsPreprocessor();
        while(rs.next()) {
            String text = preprocessor.preprocessBody(rs.getString(1), null, -1, null);
            String code = preprocessor.getCode(rs.getString(1));
            if(Stream.of(errorTerms).anyMatch(term->text.contains(term))) {
                Integer idx = null;
                while(idx==null||idx>=0) {
                    if(idx!=null) {
                        int endIdx = IntStream.of(text.indexOf("\n", idx+1), text.indexOf(",", idx+1)).filter(i->i>0).min().orElse(Integer.MAX_VALUE);
                        int startIdx = text.lastIndexOf(" ", idx);
                        if(startIdx>=1) {
                            int tmp = text.lastIndexOf(" ", startIdx-1);
                            if(tmp>=0) {
                                startIdx = tmp;
                            }
                        }
                        System.out.println("Start: "+startIdx+", End: "+endIdx);
                        String context = text.substring(Math.max(0, startIdx), Math.min(text.length(), endIdx));
                        System.out.println("Context: "+context);
                        System.out.println("Code: "+code);
                    }
                    final Integer _idx = idx;
                    idx = Stream.of(errorTerms).mapToInt(term->text.indexOf(term, _idx==null?0:(_idx+1))).filter(i->i>=0).min().orElse(-1);
                }
                System.out.println("ERROR CODE TEXT: "+text);
                hasErrors ++;
            }
            if(count%1000==999) {
                System.out.println("Count: "+count+". Num with error codes: "+hasErrors);
            }
            count++;
        }
        rs.close();
        ps.close();
        conn.close();
    }
}
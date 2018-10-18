package analysis.error_codes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class ErrorCodesModel {
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);

        PreparedStatement ps = conn.prepareStatement("select title from posts where parent_id is null");
        ps.setFetchSize(10);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        int hasErrors = 0;
        while(rs.next()) {
            String text = rs.getString(1).toLowerCase();
            if(text.contains("error code") || text.contains("errno") || text.contains("error n") || text.contains("error:") || text.contains("<error>")) {
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
package analysis.tags;

import csv.CSVHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

public class TagModel {
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        final int minTagCount = 5;
        PreparedStatement ps = conn.prepareStatement("select tags from posts where tags is not null");
        ps.setFetchSize(1000);
        ResultSet rs = ps.executeQuery();
        Map<String, Integer> tagToCountMap = new HashMap<>();
        int count = 0;
        while(rs.next()) {
            String tagStr = rs.getString(1);
            String[] tags = tagStr.split("><");
            for(String tag : tags) {
                tag = tag.replace("<", "").replace(">", "").trim().toLowerCase();
                if (tag.length() > 0) {
                    tagToCountMap.putIfAbsent(tag, 0);
                    tagToCountMap.put(tag, tagToCountMap.get(tag) + 1);
                }
            }
            if(count % 1000 == 999) {
                System.out.println("Seen: "+count+", Map size: "+tagToCountMap.size());
            }
            count++;
        }
        rs.close();
        ps.close();
        conn.close();

        System.out.println("Num tags before truncation: "+tagToCountMap.size());


        tagToCountMap = tagToCountMap.entrySet().stream()
                .filter(e->e.getValue()>=minTagCount)
                .collect(Collectors.toMap(e->e.getKey(),e->e.getValue()));

        List<String> tags = new ArrayList<>(tagToCountMap.keySet());
        Collections.sort(tags);

        System.out.println("Num distinct tags: "+tagToCountMap.size());

        CSVHelper.writeToCSV("tags.csv", tags.stream().map(e->new String[]{e}).collect(Collectors.toList()));
        CSVHelper.writeToCSV("tags_count_map.csv", tagToCountMap.entrySet().stream().map(e->new String[]{e.getKey(), e.getValue().toString()}).collect(Collectors.toList()));
    }
}

package xml;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExtractXMLToSQL {
    public static void main(String[] args) throws Exception {
       // run("stackoverflow", "stackoverflow_data");
       // run("serverfault", "serverfault_data");
        run("unix", "unix");
        run("dba", "dba");
        run("askubuntu", "askubuntu");
        run("superuser", "superuser");
    }

    public static void run(String database, String folder) throws Exception {
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/"+database+"?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        boolean posts = true;
        boolean badges = true;
        boolean comments = true;
        boolean postLinks = true;
        boolean users = true;
        boolean votes = true;
        boolean postHistory = true;

        // badges
        if (badges) {
            String filePath = "/media/ehallmark/tank/"+folder+"/Badges.xml";
            List<String> attrs = Arrays.asList("id", "user_id", "name", "date");
            List<String> attrsXML = attrs.stream().map(attr->attr.replace("_","")).collect(Collectors.toList());

            Function<String, Object[]> dataFunction = line -> {
                Document doc = convertStringToDocument(line);
                if(doc==null) return null;
                Element attributes = doc.select("row").first();
                if(attributes==null) return null;
                return new Object[]{
                        integerOr(attributes.attr(attrsXML.get(0)), null),
                        integerOr(attributes.attr(attrsXML.get(1)), null),
                        attributes.attr(attrsXML.get(2)),
                        dateOr(attributes.attr(attrsXML.get(3)), null)
                };

            };
            iterateLines(filePath, createLineConsumer(conn, attrs, Collections.singletonList("id"), "badges", dataFunction));
            conn.commit();
        }
        // comments
        if (comments) {
            String filePath = "/media/ehallmark/tank/"+folder+"/Comments.xml";
            List<String> attrs = Arrays.asList("id", "post_id", "score", "text", "creation_date", "user_id");
            List<String> attrsXML = attrs.stream().map(attr->attr.replace("_","")).collect(Collectors.toList());

            Function<String, Object[]> dataFunction = line -> {
                Document doc = convertStringToDocument(line);
                if(doc==null) return null;
                Element attributes = doc.select("row").first();
                if(attributes==null) return null;
                return new Object[]{
                        integerOr(attributes.attr(attrsXML.get(0)), null),
                        integerOr(attributes.attr(attrsXML.get(1)), null),
                        integerOr(attributes.attr(attrsXML.get(2)), null),
                        attributes.attr(attrsXML.get(3)),
                        dateOr(attributes.attr(attrsXML.get(4)), null),
                        integerOr(attributes.attr(attrsXML.get(5)), null)
                };

            };
            iterateLines(filePath, createLineConsumer(conn, attrs, Collections.singletonList("id"), "comments", dataFunction));
            conn.commit();
        }

        // Users
        if (postHistory) {
            String filePath = "/media/ehallmark/tank/"+folder+"/PostHistory.xml";
            List<String> attrs = Arrays.asList("id", "post_history_type_id", "post_id", "revision_guid", "creation_date", "user_id", "user_display_name", "comment", "text", "close_reason_id");
            List<String> attrsXML = attrs.stream().map(attr->attr.replace("_","")).collect(Collectors.toList());

            Function<String, Object[]> dataFunction = line -> {
                Document doc = convertStringToDocument(line);
                if(doc==null) return null;
                Element attributes = doc.select("row").first();
                if(attributes==null) return null;
                return new Object[]{
                        integerOr(attributes.attr(attrsXML.get(0)), null),
                        integerOr(attributes.attr(attrsXML.get(1)), null),
                        integerOr(attributes.attr(attrsXML.get(2)), null),
                        attributes.attr(attrsXML.get(3)),
                        dateOr(attributes.attr(attrsXML.get(4)), null),
                        integerOr(attributes.attr(attrsXML.get(5)), null),
                        attributes.attr(attrsXML.get(6)),
                        attributes.attr(attrsXML.get(7)),
                        attributes.attr(attrsXML.get(8)),
                        integerOr(attributes.attr(attrsXML.get(9)), null)
                };

            };
            iterateLines(filePath, createLineConsumer(conn, attrs, Collections.singletonList("id"), "post_history", dataFunction));
            conn.commit();
        }


        // Post Links
        if (postLinks) {
            String filePath = "/media/ehallmark/tank/"+folder+"/PostLinks.xml";
            List<String> attrs = Arrays.asList("id", "creation_date", "post_id", "related_post_id", "link_type_id");
            List<String> attrsXML = attrs.stream().map(attr->attr.replace("_","")).collect(Collectors.toList());

            Function<String, Object[]> dataFunction = line -> {
                Document doc = convertStringToDocument(line);
                if(doc==null) return null;
                Element attributes = doc.select("row").first();
                if(attributes==null) return null;
                return new Object[]{
                        integerOr(attributes.attr(attrsXML.get(0)), null),
                        dateOr(attributes.attr(attrsXML.get(1)), null),
                        integerOr(attributes.attr(attrsXML.get(2)), null),
                        integerOr(attributes.attr(attrsXML.get(3)), null),
                        integerOr(attributes.attr(attrsXML.get(4)), null)
                };

            };
            iterateLines(filePath, createLineConsumer(conn, attrs, Collections.singletonList("id"), "post_links", dataFunction));
            conn.commit();
        }

        // Users
        if (users) {
            String filePath = "/media/ehallmark/tank/"+folder+"/Users.xml";
            List<String> attrs = Arrays.asList("id", "reputation", "creation_date", "display_name", "email_hash", "last_access_date", "website_url", "location", "age", "about_me", "views", "up_votes", "down_votes");
            List<String> attrsXML = attrs.stream().map(attr->attr.replace("_","")).collect(Collectors.toList());

            Function<String, Object[]> dataFunction = line -> {
                Document doc = convertStringToDocument(line);
                if(doc==null) return null;
                Element attributes = doc.select("row").first();
                if(attributes==null) return null;
                return new Object[]{
                        integerOr(attributes.attr(attrsXML.get(0)), null),
                        integerOr(attributes.attr(attrsXML.get(1)), null),
                        dateOr(attributes.attr(attrsXML.get(2)), null),
                        attributes.attr(attrsXML.get(3)),
                        attributes.attr(attrsXML.get(4)),
                        dateOr(attributes.attr(attrsXML.get(5)), null),
                        attributes.attr(attrsXML.get(6)),
                        attributes.attr(attrsXML.get(7)),
                        integerOr(attributes.attr(attrsXML.get(8)), null),
                        attributes.attr(attrsXML.get(9)),
                        integerOr(attributes.attr(attrsXML.get(10)), null),
                        integerOr(attributes.attr(attrsXML.get(11)), null),
                        integerOr(attributes.attr(attrsXML.get(12)), null)
                };

            };
            iterateLines(filePath, createLineConsumer(conn, attrs, Collections.singletonList("id"), "users", dataFunction));
            conn.commit();
        }

        // Users
        if (votes) {
            String filePath = "/media/ehallmark/tank/"+folder+"/Votes.xml";
            List<String> attrs = Arrays.asList("id", "post_id", "vote_type_id", "creation_date", "user_id", "bounty_amount");
            List<String> attrsXML = attrs.stream().map(attr->attr.replace("_","")).collect(Collectors.toList());

            Function<String, Object[]> dataFunction = line -> {
                Document doc = convertStringToDocument(line);
                if(doc==null) return null;
                Element attributes = doc.select("row").first();
                if(attributes==null) return null;
                return new Object[]{
                        integerOr(attributes.attr(attrsXML.get(0)), null),
                        integerOr(attributes.attr(attrsXML.get(1)), null),
                        integerOr(attributes.attr(attrsXML.get(2)), null),
                        dateOr(attributes.attr(attrsXML.get(3)), null),
                        integerOr(attributes.attr(attrsXML.get(4)), null),
                        integerOr(attributes.attr(attrsXML.get(5)), null)
                };

            };
            iterateLines(filePath, createLineConsumer(conn, attrs, Collections.singletonList("id"), "votes", dataFunction));
            conn.commit();
        }

        // posts
        if (posts) {
            String filePath = "/media/ehallmark/tank/"+folder+"/Posts.xml";
            List<String> attrs = Arrays.asList("id", "post_type_id", "parent_id", "accepted_answer_id", "creation_date", "score", "view_count", "body", "owner_user_id", "last_editor_user_id", "last_editor_display_name", "last_edit_date", "last_activity_date", "closed_date", "title", "tags", "answer_count", "comment_count", "favorite_count");
            List<String> attrsXML = attrs.stream().map(attr->attr.replace("_","")).collect(Collectors.toList());

            Function<String, Object[]> dataFunction = line -> {
                Document doc = convertStringToDocument(line);
                if(doc==null) return null;
                Element attributes = doc.select("row").first();
                if(attributes==null) return null;
                return new Object[]{
                        integerOr(attributes.attr(attrsXML.get(0)), null),
                        integerOr(attributes.attr(attrsXML.get(1)), null),
                        integerOr(attributes.attr(attrsXML.get(2)), null),
                        integerOr(attributes.attr(attrsXML.get(3)), null),
                        dateOr(attributes.attr(attrsXML.get(4)), null),
                        integerOr(attributes.attr(attrsXML.get(5)), null),
                        integerOr(attributes.attr(attrsXML.get(6)), null),
                        attributes.attr(attrsXML.get(7)),
                        integerOr(attributes.attr(attrsXML.get(8)), null),
                        integerOr(attributes.attr(attrsXML.get(9)), null),
                        attributes.attr(attrsXML.get(10)),
                        dateOr(attributes.attr(attrsXML.get(11)), null),
                        dateOr(attributes.attr(attrsXML.get(12)), null),
                        dateOr(attributes.attr(attrsXML.get(13)), null),
                        attributes.attr(attrsXML.get(14)),
                        attributes.attr(attrsXML.get(15)),
                        integerOr(attributes.attr(attrsXML.get(16)), null),
                        integerOr(attributes.attr(attrsXML.get(17)), null),
                        integerOr(attributes.attr(attrsXML.get(18)), null),
                };

            };
            iterateLines(filePath, createLineConsumer(conn, attrs, Collections.singletonList("id"), "posts", dataFunction));
            conn.commit();
        }

        conn.commit();
        for(PreparedStatement ps : statements) {
            try {
                ps.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        conn.close();
    }

    private static Timestamp dateOr(String val, Timestamp defaultVal) {
        try {
            return Timestamp.valueOf(LocalDateTime.parse(val, DateTimeFormatter.ISO_DATE_TIME));
        } catch(Exception e) {
            return defaultVal;
        }
    }

    private static Integer integerOr(String val, Integer defaultVal) {
        try {
            return Integer.valueOf(val);
        } catch(Exception e) {
            return defaultVal;
        }
    }
    private static List<PreparedStatement> statements = new ArrayList<>();
    private static Consumer<String> createLineConsumer(Connection conn, List<String> attrs, List<String> primaryKeys, String tableName, Function<String, Object[]> dataFunction) throws SQLException {
        PreparedStatement ps = createStatement(conn, attrs, primaryKeys, tableName);
        statements.add(ps);
        AtomicLong counter = new AtomicLong(0);
        AtomicLong valid = new AtomicLong(0);
        return line -> {
            Object[] data = dataFunction.apply(line);
            if(data!=null) {
                try {
                    for (int i = 0; i < data.length; i++) {
                        ps.setObject(i + 1, data[i]);
                    }
                    ps.executeUpdate();
                    ps.clearParameters();
                    valid.getAndIncrement();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if(counter.getAndIncrement() % 1000==999) {
                System.out.println("Seen "+tableName+": "+counter.get() +" Valid: "+valid.get());
                try {
                    conn.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Unable to commit to postgres...");
                    System.exit(1);
                }
            }
        };
    }


    private static Document convertStringToDocument(String xmlStr) {
        return Jsoup.parse(xmlStr);
    }

    private static PreparedStatement createStatement(Connection conn, List<String> fields, List<String> primaryKeys, String table) throws SQLException {
        StringJoiner valJoin = new StringJoiner(",");
        for(int i = 0; i < fields.size(); i++) valJoin = valJoin.add("?");
        String valStr = valJoin.toString();
        List<String> nonPrimaryKeys = new ArrayList<>(fields);
        for(String primaryKey : primaryKeys) {
            nonPrimaryKeys.remove(primaryKey);
        }
        return conn.prepareStatement("insert into "+table+" ("+String.join(",",fields)+") values ("+valStr+") on conflict ("+String.join(",", primaryKeys)+") do update set ("+String.join(",",nonPrimaryKeys)+") = ("+String.join(",", nonPrimaryKeys.stream().map(p->"excluded."+p).collect(Collectors.toList()))+")");
    }


    private static void iterateLines(String filePath, Consumer<String> lineHandler) throws IOException {
        LineIterator iterator = FileUtils.lineIterator(new File(filePath), "UTF-8");
        iterator.next();
        iterator.next(); // skip 2 lines
        while(iterator.hasNext()) {
            lineHandler.accept(iterator.next());
        }
        iterator.close();
    }
}

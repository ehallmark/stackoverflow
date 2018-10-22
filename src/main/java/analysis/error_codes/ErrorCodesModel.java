package analysis.error_codes;

import analysis.min_hash.MinHash;
import analysis.preprocessing.PostsPreprocessor;
import com.opencsv.CSVWriter;
import csv.CSVHelper;
import javafx.util.Pair;

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

public class ErrorCodesModel {
    public static final String MIN_HASH_FILE = "error_code_min_hash.jobj";
    public static final String DATA_FILE = "/media/ehallmark/tank/stack_error_code_tag_prediction_data.csv";
    public static void main(String[] args) throws Exception {
        // this class analyzes posts tags
        boolean test = false;
        boolean trainMinHash = false;
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(DATA_FILE + (test ? ".test.csv" : "")))));

        PreparedStatement ps = conn.prepareStatement("select title, body, accepted_answer_id, tags from posts where parent_id is null and accepted_answer_id is not null and closed_date is null and view_count > 100");
        ps.setFetchSize(10);
        ResultSet rs = ps.executeQuery();
        int count = 0;
        int hasErrors = 0;
        final String[] errorTerms = new String[]{"error code:", "errorcode:", "trace:", "errcode:", "warning:", "caution:", "notice:", "errno:", "error:", "exception:", "message:", "status:", "response:"};
        final PostsPreprocessor preprocessor = new PostsPreprocessor();
        List<Pair<String,String>> errorCodes = new LinkedList<>();
        final PostsPreprocessor postsPreprocessor = new PostsPreprocessor();
        Set<String> tags = CSVHelper.readFromCSV("tags_custom.csv").stream().map(s->s[0]).collect(Collectors.toSet());
        long totalCodeLength = 0;
        long totalCodeCount = 0;
        while(rs.next()) {
            int answerId = rs.getInt(3);
            String text = rs.getString(1).toLowerCase()+"\n"+String.join("\n", preprocessor.textParts(rs.getString(2)));
            //System.out.println("Text: "+text);
            //String code = preprocessor.getCode(rs.getString(1));
            final String tagStr = rs.getString(4);
            if(Stream.of(errorTerms).anyMatch(term->text.contains(term))) {
                Integer idx = null;
                StringJoiner errorContexts = new StringJoiner(" ");
                while(idx==null||idx>=0) {
                    int endIdx;
                    if (idx != null) {
                        endIdx = IntStream.of(text.indexOf("\n", idx + 1)).filter(i -> i > 0).min().orElse(text.length());
                        int startIdx = text.lastIndexOf(" ", idx);
                        if (startIdx >= 1) {
                            int tmp = Math.max(text.lastIndexOf(" ", startIdx - 1), text.lastIndexOf("\n", startIdx - 1));
                            if (tmp >= 0) {
                                startIdx = tmp;
                            }
                        }
                        //System.out.println("Start: "+startIdx+", End: "+endIdx);
                        String context = text.substring(Math.max(0, startIdx), Math.min(text.length(), endIdx)).replace("\n", " ").replace("  ", " ").trim();
                        //System.out.println("Context: "+context);
                        errorContexts.add(context);
                        totalCodeLength += context.length();
                        totalCodeCount ++;
                        // System.out.println("Code: "+code);
                        if (endIdx < 0) {
                            endIdx = text.length();
                        }
                        //System.out.println("END IDX FOR "+answerId+": "+endIdx);

                        // write dataset for nn
                        final Object[] questionFeatures = postsPreprocessor.getCodeCharFeaturesFor(context, tagStr, tags);
                        if (questionFeatures != null && questionFeatures[1].toString().trim().length() > 0) {
                            final String[] featuresPos = new String[]{
                                    String.join(" ", IntStream.of((int[])questionFeatures[0]).mapToObj(d->String.valueOf(d)).collect(Collectors.toList())),
                                    questionFeatures[1].toString()
                            };
                            writer.writeNext(featuresPos);
                        }
                    } else {
                        endIdx = -2;
                    }
                    final Integer _idx = endIdx + 1;
                    if(endIdx>=text.length()) break;
                    idx = Stream.of(errorTerms).mapToInt(term->text.indexOf(term, (_idx+1))).filter(i->i>=0).min().orElse(-1);
                }
                errorCodes.add(new Pair<>(String.valueOf(answerId), errorContexts.toString()));
                //System.out.println("ERROR CODE TEXT: "+text);
                hasErrors ++;
            }
            if(count%1000==999) {
                System.out.println("Count: "+count+". Num with error codes: "+hasErrors+", Avg code length: "+(new Double(totalCodeLength)/totalCodeCount));
                writer.flush();
            }
            if(test && count > 100000) break;
            count++;
        }
        rs.close();
        ps.close();
        conn.close();
        writer.flush();
        writer.close();
        if(trainMinHash) {
            MinHash hash = new MinHash(256, 3, 5, 7);
            System.out.println("Fitting hash...");
            hash.initialize(errorCodes);
            System.out.println("Saving hash...");
            hash.save(MIN_HASH_FILE);
            hash = MinHash.load(MIN_HASH_FILE);
            System.out.println("Saved.");
            for (Pair<String, String> context : errorCodes) {
                System.out.println("Most similar to " + context.getValue() + ": \n\t" + String.join("\n\t", hash.mostSimilar(context.getValue(), 5).stream().map(p -> p.getValue().toString() + ": " + p.getKey()).collect(Collectors.toList())));
            }
        }
    }
}
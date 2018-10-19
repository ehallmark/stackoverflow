package analysis.preprocessing;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Stream;

public class PostsPreprocessor {

    public Object[] getFeaturesFor(ResultSet rs, int startIdx, Map<String,Integer> wordIdxMap, Set<String> availableTags) throws SQLException {
        String body = preprocessBody(rs.getString(startIdx+1),wordIdxMap, 256);
        String tagStr = rs.getString(startIdx+2);
        String[] tags = Stream.of(tagStr.split("><"))
                .map(s->s.replace("<","").replace(">",""))
                .filter(s->s.length()>0 && availableTags.contains(s))
                .toArray(s->new String[s]);
        String title = preprocess(rs.getString(startIdx+3),wordIdxMap, 32);
        return new Object[]{
                body,
                ("<"+String.join("><", tags)+">").replace("<>",""),
                title
        };
    }

    public String preprocess(String in, Map<String,Integer> vocabIndexMap, int limit) {
        String[] words = in.toLowerCase().replaceAll("[^a-z0-9 ]", " ").split("\\s+");
        StringJoiner sj = new StringJoiner(",");
        int i = 0;
        for(String word : words) {
            Integer idx = vocabIndexMap.get(word);
            if(idx!=null) {
                sj.add(idx.toString());
                i++;
                if(i>=limit) break;
            }
        }
        return sj.toString();
    }


    public String preprocessBody(String in, Map<String,Integer> vocabIndexMap, int limit) {
        Document body = Jsoup.parse(in);
        Elements code = body.select("code");
        code.remove();
        in = body.text();
        String[] words = in.toLowerCase().replaceAll("[^a-z0-9 ]", " ").split("\\s+");
        if(vocabIndexMap==null) {
            return String.join(" ", words);
        } else {
            StringJoiner sj = new StringJoiner(",");
            int i = 0;
            for (String word : words) {
                Integer idx = vocabIndexMap.get(word);
                if (idx != null) {
                    sj.add(idx.toString());
                    i++;
                    if (i >= limit) break;
                }
            }
            return sj.toString();
        }
    }

}

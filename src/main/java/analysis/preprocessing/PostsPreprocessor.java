package analysis.preprocessing;

import com.opencsv.CSVWriter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
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

    public int[] getCharsAsTimeSeries(String code, int maxTimeSteps) {
        return computeCharTimeSeries(code, allCharsToIndexMap, maxTimeSteps);
    }


    public String[] getAllFeaturesFor(ResultSet rs, int startIdx, Map<String,Integer> wordVocabIdxMap,  Map<String,Integer> codeVocabIdxMap, Set<String> availableTags) throws SQLException {
        String bodyText = rs.getString(startIdx+1);
        int id = rs.getInt(startIdx+3);
        int parentId = rs.getInt(startIdx+3);
        int acceptedAnswerId = rs.getInt(startIdx+3);
        String body = preprocessBody(bodyText,wordVocabIdxMap, 512);
        String codeText = getCode(bodyText);
        String code = preprocess(codeText, codeVocabIdxMap, 512);
        double[] charFrequency = computeCharFrequency(codeText, charToIndexMap);
        int[] chars = computeCharTimeSeries(codeText, allCharsToIndexMap, 1024);
        String tagStr = rs.getString(startIdx+2);
        String[] tags = Stream.of(tagStr.split("><"))
                .map(s->s.replace("<","").replace(">",""))
                .filter(s->s.length()>0 && availableTags.contains(s))
                .toArray(s->new String[s]);
        return new String[]{
                body,
                code,
                String.join(",", IntStream.of((int[])chars).mapToObj(d->String.valueOf(d)).collect(Collectors.toList())),
                String.join(",", DoubleStream.of((double[])charFrequency).mapToObj(d->String.valueOf(d)).collect(Collectors.toList())),
                ("<"+String.join("><", tags)+">").replace("<>",""),
                String.valueOf(id),
                String.valueOf(parentId),
                String.valueOf(acceptedAnswerId)
        };
    }


    public Object[] getCodeFeaturesFor(ResultSet rs, int startIdx, Map<String,Integer> wordIdxMap, Set<String> availableTags) throws SQLException {
        String code = preprocess(getCode(rs.getString(startIdx+1)), wordIdxMap, 1024);

        if(code.trim().isEmpty()) return null;
        String tagStr = rs.getString(startIdx+2);
        String[] tags = Stream.of(tagStr.split("><"))
                .map(s->s.replace("<","").replace(">",""))
                .filter(s->s.length()>0 && availableTags.contains(s))
                .toArray(s->new String[s]);

        double[] charFrequency = computeCharFrequency(code, charToIndexMap);
        return new Object[]{
                code,
                charFrequency,
                ("<"+String.join("><", tags)+">").replace("<>","")
        };
    }

    private static final char[] ALL_CHARS = new char[]{
            'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',
            'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
            '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', ' ', '\'', '"', '\\', '`', '~', '!', '@', '#', '$', '%',
            '^', '&', '*', '(', ')', '_', '+', '-', '=', '<', '>', '?', ',', '.', '/', '\n', '\t', ':', ';', '[', ']',
            '{', '}', '|'
    };
    private static final Map<Character, Integer> allCharsToIndexMap;
    static {
        allCharsToIndexMap = new HashMap<>();
        for(int i = 0; i < ALL_CHARS.length; i++) {
            char ch = ALL_CHARS[i];
            allCharsToIndexMap.put(ch, i);
        }
    }

    private static final char[] SPECIAL_CHARS = new char[]{' ', '\'', '"', '\\', '`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '+', '-', '=', '<', '>', '?', ',', '.', '/', '\n', '\t', ':', ';', '[', ']', '{', '}', '|'};
    private static final Map<Character, Integer> charToIndexMap;
    static {
        charToIndexMap = new HashMap<>();
        for(int i = 0; i < SPECIAL_CHARS.length; i++) {
            char ch = SPECIAL_CHARS[i];
            charToIndexMap.put(ch, i);
        }
    }

    public static void main(String[] args) throws Exception {
        {
            CSVWriter writer = new CSVWriter(new FileWriter("/home/ehallmark/repos/stackoverflow/special_chars.csv"));
            for (char ch : SPECIAL_CHARS) {
                writer.writeNext(new String[]{String.valueOf(ch)});
            }
            writer.close();
        }
        {
            CSVWriter writer = new CSVWriter(new FileWriter("/home/ehallmark/repos/stackoverflow/all_chars.csv"));
            for (char ch : ALL_CHARS) {
                writer.writeNext(new String[]{String.valueOf(ch)});
            }
            writer.close();
        }
        System.out.println("All chars: "+allCharsToIndexMap.size());
        System.out.println("Special chars: "+charToIndexMap.size());
    }
    public Object[] getCodeCharFeaturesFor(String code, String tagStr, Set<String> availableTags) throws SQLException {
        if(code.trim().isEmpty()) return null;
        String[] tags = Stream.of(tagStr.split("><"))
                .map(s->s.replace("<","").replace(">",""))
                .filter(s->s.length()>0 && availableTags.contains(s))
                .toArray(s->new String[s]);

        int[] chars = computeCharTimeSeries(code, allCharsToIndexMap, 2048);
        return new Object[]{
                chars,
                ("<"+String.join("><", tags)+">").replace("<>","")
        };
    }

    public double[] computeSpecialCharFrequency(String in) {
        return computeCharFrequency(getCode(in), charToIndexMap);
    }


    public static double[] computeCharFrequency(String in, Map<Character, Integer> map) {
        double[] freqs = new double[map.size()];
        char[] chars = in.toCharArray();
        int cnt = 0;
        for(int i = 0; i < chars.length; i++) {
            Integer idx = map.get(chars[i]);
            if(idx!=null) {
                freqs[idx] += 1;
                cnt++;
            }
        }
        if(cnt > 0) {
            for(int i = 0; i < freqs.length; i++) {
                freqs[i]/=cnt;
            }
        }
        return freqs;
    }

    public static int[] computeCharTimeSeries(String in, Map<Character, Integer> map, int t) {
        int[] indices = new int[t];
        char[] chars = in.toCharArray();
        for(int i = 0; i < Math.min(t, chars.length); i++) {
            Integer idx = map.get(chars[i]);
            if(idx!=null) {
                indices[i]  = idx + 1; // 0 index is unknown char
            }
        }
        return indices;
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
        return preprocessBody(in, vocabIndexMap, limit, "[^a-z0-9 ]");
    }

    public List<String> textParts(String in) {
        Document body = Jsoup.parse(in.toLowerCase());
        List<Element> children = body.children();
        final List<String> parts = new ArrayList<>();
        for(Element child : children) {
            textPartHelper(parts, child);
        }
        return parts;
    }

    private void textPartHelper(List<String> parts, Element element) {
        parts.add(element.text());
        for(Element child : element.children()) {
            textPartHelper(parts, child);
        }
    }

    public String preprocessBody(String in, Map<String,Integer> vocabIndexMap, int limit, String replace) {
        Document body = Jsoup.parse(in);
        Elements code = body.select("code");
        code.remove();
        in = body.text().toLowerCase();
        if(replace!=null) {
            in = in.replaceAll(replace, " ");
        }
        if(vocabIndexMap==null) {
            return in;
        } else {
            String[] words = in.split("\\s+");
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


    public String getCode(String in) {
        Document body = Jsoup.parse(in);
        Elements code = body.select("code");
        return code.text();
    }

}

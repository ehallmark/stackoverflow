package analysis.python;

import analysis.preprocessing.PostsPreprocessor;
import com.google.gson.Gson;
import javafx.util.Pair;
import org.bytedeco.javacv.FrameFilter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

public class PythonAdapter {

    public static String get(String urlStr, String inputParamName, String inputs, int n) throws IOException {
        if(inputs==null || inputs.isEmpty()) return null;
        URL url = new URL(urlStr + "?n="+n+"&"+inputParamName+"="+URLEncoder.encode(inputs, "UTF-8"));
        System.out.println("URL: "+url);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");

        Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

        StringBuilder sb = new StringBuilder();
        for (int c; (c = in.read()) >= 0;)
            sb.append((char)c);
        String response = sb.toString();
        in.close();
        return response;
    }

    public static List<Pair<String,Double>> predictTags(String error, int n) {
        final String url = "http://127.0.0.1:8081/predict_tags";
        try {
            String response = get(url, "text", error, n);
            List<Map<String,Object>> results = new Gson().fromJson(response, List.class);
            List<Pair<String,Double>> ret = new ArrayList<>();
            for(Map<String,Object> result : results) {
                ret.add(new Pair<>((String)result.get("name"), Double.valueOf((String)result.get("score"))));
            }
            return ret;

        } catch(Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    public static List<Pair<String,Double>> predictCodeCharLevel(String text, int n) {
        final String url = "http://127.0.0.1:8081/predict_code";
        final PostsPreprocessor postsPreprocessor = new PostsPreprocessor();
        try {
            String response = get(url, "inputs", new Gson().toJson(postsPreprocessor.getCharsAsTimeSeries(text, 512)), n);
            List<Map<String,Object>> results = new Gson().fromJson(response, List.class);
            List<Pair<String,Double>> ret = new ArrayList<>();
            for(Map<String,Object> result : results) {
                ret.add(new Pair<>((String)result.get("name"), Double.valueOf((String)result.get("score"))));
            }
            return ret;

        } catch(Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }
}

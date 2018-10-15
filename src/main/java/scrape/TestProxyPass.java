package scrape;

import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class TestProxyPass {

    public static void main(String[] args) throws Exception {
        boolean useProxies = true;
        List<String> proxyIps = new ArrayList<>();
        int idx = 4;
        if(useProxies) {
            File proxyFile = new File("proxies.csv");
            FileUtils.copyURLToFile(new URL("http://api.buyproxies.org/?a=showProxies&pid=107699&key=03b14c878d7bd334fee346f40fd2e4e4"), proxyFile);
            String[] proxies = FileUtils.readFileToString(proxyFile, Charset.defaultCharset()).split("\\n");
            int proxiesPerMain = 10;
            int numProxies = 10;
            for (int i = idx * proxiesPerMain; i < idx * proxiesPerMain + numProxies; i++) {
                //break;
                String proxy = proxies[i];
                System.out.println("Proxy: " + proxy);
                proxyIps.add(proxy.split(":")[0]);
            }
        }
        final String urlPrefix = "https://stackoverflow.com/questions/";

        //final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/beerdb?user=postgres&password=password&tcpKeepAlive=true");
        //conn.setAutoCommit(false);
        final int bound = 20000000;
        boolean reseed = false;
        final boolean remainingOnly = true;
        final boolean ingesting = false;
        long timeSleep = 1500;

        File folder = new File("/home/ehallmark/data/stack_overflow/");
        Set<Integer> alreadySeen = Stream.of(folder.listFiles())
                .map(f->Integer.valueOf(f.getName().replace(".gzip","")))
                .collect(Collectors.toSet());

        List<Integer> indices = new ArrayList<>();
        for(int i = 1; i <= bound; i++) {
            if(!remainingOnly || !alreadySeen.contains(i)) {
                indices.add(i);
            }
        }
        Collections.shuffle(indices, new Random(System.currentTimeMillis()));
        for(int ip = 0; ip < proxyIps.size(); ip++) {
            final String proxyIp = proxyIps.get(ip);
            List<Integer> proxyIndices = indices.subList(ip*bound/proxyIps.size(), Math.min(indices.size(),(ip+1)*bound/proxyIps.size()));
            System.out.println("Num proxy indices: "+proxyIndices.size());
            System.out.println("Num remaining: "+proxyIndices.size());
            LongStream indicesStream = proxyIndices.stream().mapToLong(i->i);
            indicesStream.forEach(i-> {
                String url = urlPrefix + i + "/";
                File overviewFile = new File(folder, String.valueOf(i) + ".gzip");
                if (!ingesting && (!overviewFile.exists() || reseed)) {
                    try {
                        String page;
                        System.out.println("Searching for (idx: " + idx + "): " + url);
                        dump(proxyIp, url);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }


    public static Pair<String,String> dump(String proxyIp, String URLName) throws Exception {
        //System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");
        //System.setProperty("jdk.http.auth.proxying.disabledSchemes", "");
        DataInputStream di = null;
        byte [] b = new byte[1];

        // PROXY
        //System.setProperty("http.proxyHost","proxy.mydomain.local") ;
        //System.setProperty("http.proxyPort", "80") ;
        StringBuilder sb = new StringBuilder();
        URLConnection con;
        synchronized (TestProxyPass.class) {
            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new
                            PasswordAuthentication("ehallmark", "eug832hbs".toCharArray());
                }
            });
            URL ur = new URL(URLName);
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyIp, 80));
            con = (HttpURLConnection) ur.openConnection(proxy);
            con.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2");
            con.connect();
        }
        di = new DataInputStream(con.getInputStream());
        while (-1 != di.read(b, 0, 1)) {
            sb.append(new String(b));
        }
        di.close();
        String text = sb.toString();
        Document doc = Jsoup.parse(text);
        String urlStr = doc.select("head meta[property=\"og:url\"]").attr("content");
        if(urlStr==null||urlStr.isEmpty()) {
            return new Pair<>("", URLName);
        }
        System.out.println("Found URL with proxy: "+urlStr);
        return new Pair<>(text,urlStr);
    }
}

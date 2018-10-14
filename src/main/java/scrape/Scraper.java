package scrape;

import javafx.util.Pair;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.nd4j.linalg.primitives.Triple;


import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Scraper {
    public static void writeToGzip(String string, File file) throws IOException {
        BufferedWriter gz = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file))));
        gz.write(string);
        gz.flush();
        gz.close();
    }

    public static String readFromGzip(File file) throws IOException {
        BufferedReader gz = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
        StringJoiner sj = new StringJoiner("");
        String line = gz.readLine();
        while(line!=null) {
            sj.add(line);
            line = gz.readLine();
        }
        gz.close();
        return sj.toString(); //.replaceAll("\u0000", "");
    }

    public static void main(String[] args) throws Exception {
        scrapeWithProxy(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Integer.valueOf(args[2]));
    }


    static void scrapeWithProxy(int proxyIdx, final int sequential, final int numProxies) throws Exception {
        System.out.println("Starting proxy: "+proxyIdx);
        System.setProperty("webdriver.chrome.driver", "/usr/bin/chromedriver");
        System.setProperty("webdriver.firefox.driver", "/usr/bin/geckodriver");
        List<String> proxyIps = new ArrayList<>();
        File proxyFile = new File("proxies.csv"+proxyIdx);
        FileUtils.copyURLToFile(new URL("http://api.buyproxies.org/?a=showProxies&pid=107699&key=03b14c878d7bd334fee346f40fd2e4e4"), proxyFile);
        String[] proxies = FileUtils.readFileToString(proxyFile, Charset.defaultCharset()).split("\\n");
        for(int p = 0; p < proxies.length; p++) {
            String proxy = proxies[p];
            proxyIps.add(proxy.split(":")[0]);
            System.out.println("Proxy: " + proxy);
        }

        final String urlPrefix = "https://stackoverflow.com/questions/";

        //final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/beerdb?user=postgres&password=password&tcpKeepAlive=true");
        //conn.setAutoCommit(false);
        final int bound = 50000000;
        boolean reseed = false;
        final boolean ingesting = false;
        long timeSleep = 2000;
        long switchEveryNTries = 30 * 10;

        File folder = new File("/home/ehallmark/data/stack_overflow/");
        final Random rand = new Random(System.currentTimeMillis());
        int c = 0;
        boolean switched = false;
        while(true) {
            c++;
            if(c % switchEveryNTries == switchEveryNTries-1) {
                if(switched) {
                    proxyIdx -= 100;
                } else {
                    proxyIdx += 100;
                }
                switched = !switched;
            }
            int i = rand.nextInt(bound)+1;
            while(i % numProxies!=proxyIdx) i++;
            for(int j = 0; j < sequential; j++) {
                final int idIndex = i+j;
                final String url = urlPrefix + idIndex + "/";
                int pIdx = (i%numProxies)+j;
                final String proxyIp = proxyIps.get(pIdx);
                File overviewFile = new File(folder, String.valueOf(idIndex) + ".gzip");
                if (!ingesting && (!overviewFile.exists() || reseed)) {
                    try {
                        String page;
                        System.out.println("Searching for (idx: " + pIdx + "): " + url);
                        Pair<String, String> dump;
                        try {
                            dump = TestProxyPass.dump(proxyIp, url);//driver.get(url);
                        } catch (FileNotFoundException e) {
                            System.out.println("File not found: " + url);
                            dump = new Pair<>("", url);
                        } catch (Exception e) {
                            System.out.println("Too many requests... Sleeping...");
                            TimeUnit.MINUTES.sleep(30);
                            continue;
                        }
                        if (dump == null) {
                            System.out.println("Null");
                            TimeUnit.MILLISECONDS.sleep(timeSleep);
                            continue;
                        }
                        String currentUrl = dump.getValue(); //driver.getCurrentUrl();
                        System.out.println("Current url (idx: " + pIdx + "): " + currentUrl);
                        page = dump.getKey();//driver.getPageSource();
                        if (page != null && page.length() > 0) {
                            page = currentUrl + "\n" + page;
                            //Document document = Jsoup.parse(page);
                            //System.out.println("Found page: "+page);
                            if (!overviewFile.getParentFile().exists()) {
                                overviewFile.getParentFile().mkdir();
                            }
                            writeToGzip(page, overviewFile);
                        }
                        // driver.close();
                        TimeUnit.MILLISECONDS.sleep(timeSleep);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        }

    }


}

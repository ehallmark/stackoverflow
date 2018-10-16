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
        scrapeWithProxy(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Integer.valueOf(args[2]), Integer.valueOf(args[3]));
    }


    static void scrapeWithProxy(final int proxyIdx, final int sequential, final int numProxies, int minBound) throws Exception {
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
        final int bound = 52820947;
        boolean reseed = false;
        final boolean ingesting = false;
        long timeSleep = 2100;
        File folder = new File("/home/ehallmark/data/stack_overflow/");

       // Set<Integer> existingFiles = Stream.of(folder.listFiles())
       //         .map(f->Integer.valueOf(f.getName().replace(".gzip","")))
       //         .collect(Collectors.toSet());

        System.out.println("Min bound: "+minBound);
        final Random rand = new Random(System.currentTimeMillis());
        int c = 0;
        while(true) {
            c += sequential;
            int i = proxyIdx == 0 ? c : (minBound + rand.nextInt(bound-minBound)+1);
            //if(i % numProxies==proxyIdx) {
                for (int j = 0; j < sequential; j++) {
                    final int idIndex = i + j;
                   // System.out.println("FOUND idIndex: "+idIndex);
                    final String url = urlPrefix + idIndex + "/";
                    int pIdx = (proxyIdx+j) % numProxies;
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
                            String currentUrl = dump.getValue(); //driver.getCurrentUrl();
                            System.out.println("Current url (idx: " + pIdx + "): " + currentUrl);
                            page = dump.getKey();//driver.getPageSource();
                            if (page != null) {
                                page = currentUrl + "\n" + page;
                                //Document document = Jsoup.parse(page);
                                //System.out.println("Found page: "+page);
                                writeToGzip(page, overviewFile);
                            } else {
                                System.out.println("NULLLLLL");
                            }
                            // driver.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        TimeUnit.MILLISECONDS.sleep(timeSleep);
                    }
                }
            //}
        }

    }


}

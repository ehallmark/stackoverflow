package scrape;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeDriverService;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ScrapeElastico {
    public static String readStringFromURL(String requestURL) throws IOException
    {
        URL website = new URL(requestURL);
        URLConnection connection = website.openConnection();
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2");

        BufferedReader in = new BufferedReader(
                new InputStreamReader(
                        connection.getInputStream()));
        StringBuilder response = new StringBuilder();
        String inputLine;

        while ((inputLine = in.readLine()) != null)
            response.append(inputLine);

        in.close();

        return response.toString();
    }


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
        return sj.toString();
    }

    public static final String DATA_FOLDER = "/media/ehallmark/tank/data/elastico/";

    public static void main(String[] args) throws Exception {
        scrape("https://discuss.elastic.co/t//", DATA_FOLDER, 174533, true,
                true, false, false, false);
    }

    static {
        System.setProperty("webdriver.chrome.driver", "/usr/bin/chromedriver");
       // System.setProperty("webdriver.chrome.driver", "/Users/inamo/Downloads/chromedriver_win32/chromedriver.exe");
        System.setProperty("webdriver.firefox.driver", "/usr/bin/geckodriver");
     //   chromeDriverService = new ChromeDriverService.Builder()
      //          .usingDriverExecutable(new File("/usr/bin/chromedriver"))
     //          // .usingDriverExecutable(new File("/Users/inamo/Downloads/chromedriver_win32/chromedriver.exe"))
       //         .usingAnyFreePort()
      //          .build();
    }
    public static WebDriver newWebDriver() throws Exception {
    //    if(!chromeDriverService.isRunning()) chromeDriverService.start();
        try {
            ChromeOptions options = new ChromeOptions();
            options.addArguments("--headless");
            //options.setExperimentalOption("prefs", Collections.singletonMap("profile.block_third_party_cookies", true));
            return new ChromeDriver(options);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to open driver.");
        }
    }

    public static void clearCookies(WebDriver driver) {
        while(true) {
            try {
                driver.manage().deleteAllCookies();
                break;
            } catch(Exception e) {
                e.printStackTrace();
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch(Exception e2) {

                }
            }
        }
    }

    public static void scrape(final String urlPrefix, final String folderName, final int bound,
                              boolean useDriver, boolean random, boolean remainingOnly, boolean ingesting, boolean reseed) throws Exception {
        final Connection conn = null;//DriverManager.getConnection("jdbc:postgresql://localhost/beerdb?user=postgres&password=password&tcpKeepAlive=true");
        //conn.setAutoCommit(false);
        File folder = new File(folderName);
        Set<Integer> alreadySeen = Stream.of(folder.listFiles())
                .map(f->Integer.valueOf(f.getName().replace(".gzip","")))
                .collect(Collectors.toSet());
        Set<Integer> alreadyIngested = new HashSet<>();
       // PreparedStatement ps = conn.prepareStatement("select id from wines");
       // ResultSet rs = ps.executeQuery();
        final WebDriver driver = useDriver ? newWebDriver() : null;
        try {
         //   while (rs.next()) {
         //       alreadyIngested.add(rs.getInt(1));
         //   }
         //   rs.close();
         //   ps.close();
            List<Integer> indices = new ArrayList<>();
            for (int i = 1; i <= bound; i++) {
                if (!remainingOnly || !alreadySeen.contains(i)) {
                    if (ingesting && alreadyIngested.contains(i)) {
                        continue;
                    }
                    indices.add(i);
                }
            }
            System.out.println("Num remaining: " + indices.size());
            final Random rand = new Random(System.currentTimeMillis());
            if (random) {
                Collections.shuffle(indices, rand);
            }
            //ForkJoinPool service = new ForkJoinPool(1);
            for(int i : indices) {
                //while(service.getQueuedSubmissionCount() > 100 || service.getActiveThreadCount() > 100) {
                //    System.out.println("WAITING FOR QUEUE: "+service.getQueuedSubmissionCount());
                //    TimeUnit.MILLISECONDS.sleep(1000);
                //}
                //service.execute(()->{
                    int retry = 1;
                    while(retry == 1) {
                        retry = 0;
                        System.out.println("Starting: " + i);
                        final String url = urlPrefix + i;
                        File overviewFile = new File(folder, String.valueOf(i) + ".gzip");
                        if (!ingesting && (!overviewFile.exists() || reseed)) {
                            //WebDriver driver = null;
                            //clearCookies(driver);
                            String page;
                            try {
                                System.out.println("Searching for: " + url);
                                try {
                                    //driver.get(url);
                                    if(useDriver) {
                                        driver.get(url);
                                       // TimeUnit.MILLISECONDS.sleep(10);
                                        page = driver.getPageSource();
                                        clearCookies(driver);
                                    } else {
                                        page = readStringFromURL(url);
                                    }
                                    Document doc = Jsoup.parse(page);

                                   // System.out.println("Title: " + doc.html());
                                    System.out.println("URL: " + doc.select("title").text());//driver.getCurrentUrl());

                                    //  System.out.println("Page: " + page);
                                } catch (Exception e) {
                                    page = null;
                                    System.out.println("Could not find: " + url);
                                }

                                if (page != null && page.length() > 0) {
                                    System.out.println("FOUND");
                                    //  System.out.println("Found products: " + id);
                                    writeToGzip(page, overviewFile);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        if (overviewFile.exists()) {
                            try {
                                String page = readFromGzip(overviewFile);
                                Document doc = Jsoup.parse(page);
                                if(handleQuestion(doc)) {
                                    System.out.println("RETRYING: "+url);
                                    retry ++;
                                    overviewFile.delete();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                return;
                            }
                        }
                    }
                //});
            }
            //service.shutdown();
            //service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(driver!=null) {
                    driver.quit();
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
            if(conn!=null) {
                conn.commit();
                conn.close();
            }
        }
    }

    // right now for retrying...
    private static boolean handleQuestion(Document doc) {
        String title = doc.select(".title-wrapper .fancy-title").text().trim();
        String titleHeader = doc.select("title").text().trim();
        String category = doc.select(".topic-category .category-name").text();
        if(titleHeader.isEmpty()||titleHeader.equals("Discuss the Elastic Stack")) {
            // invalid
            return false;
        }
        if(title.isEmpty() || category.isEmpty()) {
            return true;
        }
        System.out.println("Title: "+title+"\nCategory: "+category);
        return false;
    }
}

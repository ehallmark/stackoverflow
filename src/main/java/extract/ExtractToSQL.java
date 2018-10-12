package extract;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import scrape.Scraper;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

public class ExtractToSQL {

    public static void main(String[] args) throws Exception {
        final int bound = 50000000;
        final File folder = new File("/home/ehallmark/data/stack_overflow/");
        final AtomicLong cnt = new AtomicLong(0);
        for(int i = 1; i < bound; i++) {
            File file = new File(folder, String.valueOf(i) + ".gzip");
            if(file.exists()) {
                String payload = Scraper.readFromGzip(file);
                System.out.println("Payload: "+payload);
                if(payload!=null) {
                    int xmlStart = payload.indexOf("<");
                    if(xmlStart > 0) {
                        String pageTitle = payload.substring(0, xmlStart);
                        String pageContent = payload.substring(xmlStart);
                        Document doc = Jsoup.parse(pageContent);
                        // handle page
                        handlePage(pageTitle, i, doc);
                        if(cnt.getAndIncrement()%100==9999) {
                            System.out.println("Seen: "+cnt.get());
                        }
                    }
                }
            }
        }
    }

    private static void handlePage(String pageTitle, int id, Document doc) {
        System.out.println("Title "+id+": "+pageTitle);
        Elements questionName = doc.select("a.question-hyperlink");
        System.out.println("Question: "+questionName.html());
    }
}

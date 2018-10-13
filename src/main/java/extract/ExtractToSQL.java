package extract;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import scrape.Scraper;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
        //Elements questionName = doc.select("a.question-hyperlink");
        //System.out.println("Question: "+questionName.html());

        {
            Elements post = doc.select("#question .post-layout");
            if(post.size()>0) {
                handlePost(post.get(0), true);
            }
        }
        Elements answers = doc.select("#answers .answer");
        for(Element post : answers) {
            handlePost(post, false);
        }

    }

    private static void handlePost(Element post, boolean isQuestion) {
        // handle post
        int voteCount = Integer.valueOf(post.select(".vote-count-post").text());
        String text = post.select(".post-text").text();
        String html = post.select(".post-text").html();
        boolean accepted = post.hasClass("accepted-answer");
        LocalDate answerDate = LocalDate.parse(
                post.select(".user-action-time .relativetime[title]").attr("title").split(" ")[0],
                DateTimeFormatter.ISO_DATE);

        Elements userDetails = post.select(".user-details");
        boolean isCommunity = userDetails.select(".community-wiki").size()>0;
        if(isCommunity) {
            System.out.println("Community");
        } else {
            for (Element userDetail : userDetails) {
                String user = userDetail.child(0).text();
                String repStr = userDetail.select(".reputation-score").text().toLowerCase().trim();
                System.out.println("repStr: " + repStr);
                boolean hasK = repStr.contains("k");
                boolean hasM = repStr.contains("m");
                repStr = repStr.replace("k", "").replace("m", "").replace(",", "");
                int userReputation;
                try {
                    userReputation = Math.round(Float.valueOf(repStr) * (hasK ? 1000 : 1) * (hasM ? 1000000 : 1));
                } catch (Exception nfe) {
                    try {
                        userReputation = Integer.valueOf(repStr) * (hasK ? 1000 : 1) * (hasM ? 1000000 : 1);
                    } catch(Exception nfe2) {
                        //nfe.printStackTrace();
                        userReputation = 0;
                    }
                }
                System.out.println("user: " + user);
                System.out.println("userReputation: " + userReputation);
            }
        }

        System.out.println("voteCount: "+ voteCount);
        System.out.println("accepted: "+ accepted);
        System.out.println("answerDate: "+ answerDate);
        System.out.println("text: "+ text);
    }

}

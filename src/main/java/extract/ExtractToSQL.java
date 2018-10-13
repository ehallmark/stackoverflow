package extract;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;
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
            //    System.out.println("Payload: "+payload);
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
        String questionName = doc.select("#question-header a.question-hyperlink").text().trim();
        String pageHref = doc.select("#question-header a.question-hyperlink").attr("href");
        System.out.println("Question: "+questionName);
        boolean closed = questionName.endsWith("[closed]");
        Elements statusDiv = doc.select("#question .special-status .question-status");
        if(statusDiv.size()>0) {
            String status = statusDiv.select("h2 b").first().text().trim();
            String reason = statusDiv.select("p").first().textNodes().get(0).text();
            Elements statusUsers = statusDiv.select("a[href]");
            System.out.println("Status: "+status);
            System.out.println("Status reason: "+reason);
            for(Element statusUser : statusUsers) {
                String user = statusUser.text().trim();
                System.out.println("Status user: "+user);
            }
        }
        System.out.println("Is Closed: "+closed);
        {
            Elements post = doc.select("#question .post-layout");
            if(post.size()>0) {
                handlePost(post.get(0), true);
            } else {
                return;
            }
        }
        Elements answers = doc.select("#answers .answer");
        for(Element post : answers) {
            handlePost(post, false);
        }

        // qinfo
        {
            Elements qInfo = doc.select("#qinfo");
            int numViews = Integer.valueOf(qInfo.select("tr").get(1).select("td").get(1).text().split(" ")[0].replace(",",""));
            System.out.println("Num views: "+numViews);
        }
        // links
        {
            Elements links = doc.select(".sidebar-linked .spacer").not(".more");
            int numLinked = links.size();
            for(Element assoc : links) {
                int assocId = Integer.valueOf(assoc.select("a.question-hyperlink").attr("href").split("/")[4]);
            //    System.out.println("Link id: "+assocId);
            }
          //  System.out.println("Num links: "+numLinked);

        }
        // related
        {
            Elements related = doc.select(".sidebar-related .spacer").not(".more");
            int numRelated = related.size();
            for(Element assoc : related) {
                int assocId = Integer.valueOf(assoc.select("a.question-hyperlink").attr("href").split("/")[4]);
            //    System.out.println("Related id: "+assocId);
            }
          //  System.out.println("Num related: "+numRelated);

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
             //   System.out.println("repStr: " + repStr);
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
               // System.out.println("user: " + user);
              //  System.out.println("userReputation: " + userReputation);
            }
        }

      //  System.out.println("voteCount: "+ voteCount);
      //  System.out.println("accepted: "+ accepted);
      //  System.out.println("answerDate: "+ answerDate);
       // System.out.println("text: "+ text);

        // comments
        Elements comments = post.select(".comments .comment");
        for(Element comment : comments) {
            String commentText = comment.select(".comment-body .comment-copy").text();
            String commentHtml = comment.select(".comment-body .comment-copy").html();
            LocalDate commentDate = LocalDate.parse(
                    comment.select(".comment-date .relativetime-clean[title]").attr("title").split(" ")[0],
                    DateTimeFormatter.ISO_DATE);
            Elements userDetail = comment.select(".comment-user");
            String user = userDetail.text().trim();
            String repStr = userDetail.attr("title").toLowerCase().split(" ")[0];
           // System.out.println("repStr: " + repStr);
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
          //  System.out.println("Comment user: " + user);
         //   System.out.println("Comment userReputation: " + userReputation);
          //  System.out.println("Comment commentText: " + commentText);
        //    System.out.println("Comment commentDate: " + commentDate);
        }
    }

}

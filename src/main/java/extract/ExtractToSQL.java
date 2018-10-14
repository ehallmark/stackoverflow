package extract;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import scrape.Scraper;
import java.io.File;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
public class ExtractToSQL {

    public static void main(String[] args) throws Exception {
        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);
        final int bound = 50000000;
        final File folder = new File("/home/ehallmark/data/stack_overflow/");
        for(int i = 642300; i < bound; i++) {
            File file = new File(folder, String.valueOf(i) + ".gzip");
            if(file.exists()) {
                String payload;
                try {
                    payload = Scraper.readFromGzip(file);
                } catch(Exception e) {
                    e.printStackTrace();
                    System.out.println("Invalid file: "+file.getName());
                    continue;
                }
                if(payload!=null) {
                    int xmlStart = payload.indexOf("<");
                    if(xmlStart > 0) {
                        String pageTitle = payload.substring(0, xmlStart);
                        String pageContent = payload.substring(xmlStart);
                        Document doc = Jsoup.parse(pageContent);
                        // handle page
                        try {
                            handlePage(pageTitle, i, doc, conn);
                            if (i % 100 == 99) {
                                System.out.println("Seen: " + i);
                                conn.commit();
                            }
                        } catch(Exception e) {
                            e.printStackTrace();
                            System.exit(0);
                        }
                    }
                }
            }
        }
        conn.commit();
        conn.close();
    }

    private static void handlePage(String pageTitle, int id, Document doc, Connection conn) throws SQLException {
        String questionName = doc.select("#question-header a.question-hyperlink").text().trim();
        String pageHref = doc.select("#question-header a.question-hyperlink").attr("href");
        PreparedStatement questionPs = conn.prepareStatement("insert into questions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on conflict do nothing");
        {
            Elements post = doc.select("#question .post-layout");
            if (post.size() > 0) {
                // qinfo
                Elements qInfo = doc.select("#qinfo");
                int numViews;
                try {
                    numViews = Integer.valueOf(qInfo.select("tr").get(1).select("td").get(1).text().split(" ")[0].replace(",", ""));
                } catch(Exception e) {
                    numViews = 0;
                }
                boolean closed = questionName.endsWith("[closed]");
                questionPs.setInt(1, id);
                int baseQuestionId = Integer.valueOf(pageHref.split("/")[2]);
                questionPs.setInt(2, baseQuestionId);
                questionPs.setString(3, pageTitle);
                questionPs.setString(4, questionName);
                questionPs.setString(5, pageHref);
                questionPs.setBoolean(6, closed);
                Elements statusDiv = doc.select("#question .special-status .question-status");
                String status = null;
                String reason = null;
                if (statusDiv.size() > 0) {
                    status = statusDiv.select("h2 b").first().text().trim();
                    try {
                        reason = statusDiv.select("p").first().textNodes().get(0).text();
                    } catch(Exception e) {
                        reason = null;
                    }
                    Elements statusUsers = statusDiv.select("a[href]");
                    PreparedStatement ps = conn.prepareStatement("insert into status_users values (?,?) on conflict do nothing");
                    for (Element statusUser : statusUsers) {
                        String user = statusUser.text().trim();
                        ps.setInt(1, id);
                        ps.setString(2, user);
                        ps.executeUpdate();
                    }
                    ps.close();
                }
                questionPs.setString(7, status);
                questionPs.setString(8, reason);
                questionPs.setInt(20, numViews);
                handlePost(post.get(0), id, true, 8, questionPs, conn);
                //System.out.println("PS: " + questionPs.toString());
                questionPs.executeUpdate();
                questionPs.close();
            } else {
                return;
            }
        }
        Elements answers = doc.select("#answers .answer");
        if(answers.size()>0) {
            PreparedStatement answerPs = conn.prepareStatement("insert into answers values (?,?,?,?,?,?,?,?,?,?,?,?,?,?) on conflict do nothing");
            int idx = 0;
            for (Element post : answers) {
                int answerId = Integer.valueOf(post.attr("id").split("-")[1].trim());
                answerPs.setInt(1, answerId);
                answerPs.setInt(2, id);
                answerPs.setInt(3, idx);
                handlePost(post, id, false, 3, answerPs, conn);
                answerPs.executeUpdate();
                idx++;
            }
            answerPs.close();
        }

        // links
        {
            PreparedStatement ps = conn.prepareStatement("insert into question_links values (?,?) on conflict do nothing");
            Elements links = doc.select(".sidebar-linked .spacer").not(".more");
            for(Element assoc : links) {
                //System.out.println("Linked: "+assoc.html());
                try {
                    int assocId = Integer.valueOf(assoc.select("a.question-hyperlink").attr("href").replace("https://stackoverflow.com", "").split("/")[2]);
                    ps.setInt(1, id);
                    ps.setInt(2, assocId);
                    ps.executeUpdate();
                } catch(Exception e) {

                }
            }
            ps.close();

        }
        // related
        {
            PreparedStatement ps = conn.prepareStatement("insert into question_related values (?,?) on conflict do nothing");
            Elements related = doc.select(".sidebar-related .spacer").not(".more");
            for(Element assoc : related) {
                // System.out.println("Related: "+assoc.html());
                try {
                    int assocId = Integer.valueOf(assoc.select("a.question-hyperlink").attr("href").replace("https://stackoverflow.com", "").split("/")[2]);
                    ps.setInt(1, id);
                    ps.setInt(2, assocId);
                    ps.executeUpdate();
                } catch(Exception e) {

                }
            }
            ps.close();
        }
    }

    private static void handlePost(Element post, int questionId, boolean isQuestion, int psIdx, PreparedStatement ps, Connection conn) throws SQLException {
        // handle post
        final String commentTable;
        if(isQuestion) {
            commentTable = "question_comments";
        } else {
            commentTable = "answer_comments";
        }
        int voteCount = Integer.valueOf(post.select(".vote-count-post").text());
        String text = post.select(".post-text").text();
        String html = post.select(".post-text").html();
        boolean accepted = post.hasClass("accepted-answer");
        Elements userDetails = post.select(".user-details");
        boolean isCommunity = userDetails.select(".community-wiki").size()>0;
        psIdx++;
        ps.setInt(psIdx, voteCount);
        psIdx++;
        ps.setString(psIdx, text.replace("\u0000", ""));
        psIdx++;
        ps.setString(psIdx, html.replace("\u0000", ""));
        psIdx++;
        ps.setBoolean(psIdx, accepted);
        psIdx++;
        ps.setBoolean(psIdx, isCommunity);
        psIdx++;
        boolean foundAuthor = false;
        boolean foundEditor = false;
        if(isCommunity) {

        } else {
            for (Element userDetail : userDetails) {
                String user = userDetail.child(0).text().trim();
                String type = userDetail.siblingElements().first().textNodes().get(0).text().toLowerCase().trim();
                String repStr = userDetail.select(".reputation-score").text().toLowerCase().trim();
                boolean hasK = repStr.contains("k");
                boolean hasM = repStr.contains("m");
                LocalDate date;
                try {
                    date = LocalDate.parse(
                            userDetail.parent().select(".user-action-time .relativetime[title]").attr("title").split(" ")[0],
                            DateTimeFormatter.ISO_DATE);
                } catch(Exception e) {
                    date = null;
                }
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
                if(type.equals("answered")||type.equals("asked")) {
                    // author
                    ps.setString(psIdx, user);
                    ps.setInt(psIdx+1, userReputation);
                    ps.setDate(psIdx+2, date==null ? null : Date.valueOf(date));
                    foundAuthor = true;

                } else {
                    // edit
                    ps.setString(psIdx+3, user);
                    ps.setInt(psIdx+4, userReputation);
                    ps.setDate(psIdx+5, date==null ? null : Date.valueOf(date));
                    foundEditor = true;
                }
            }
        }
        if(!foundAuthor) {
            // get date
            LocalDate date;
            try {
                date = LocalDate.parse(
                        post.select(".user-action-time .relativetime[title]").last().attr("title").split(" ")[0],
                        DateTimeFormatter.ISO_DATE);
            } catch(Exception e) {
                date = null;
            }
            ps.setObject(psIdx, null);
            ps.setObject(psIdx+1, null);
            ps.setDate(psIdx+2, date==null ? null : Date.valueOf(date));
        }
        if(!foundEditor) {
            ps.setObject(psIdx+3, null);
            ps.setObject(psIdx+4, null);
            ps.setObject(psIdx+5, null);
        }

        // comments
        PreparedStatement commentPs = conn.prepareStatement("insert into "+commentTable+" values (?,?,?,?,?,?,?) on conflict do nothing");
        Elements comments = post.select(".comments .comment");
        for(Element comment : comments) {
            int commentId = Integer.valueOf(comment.attr("id").split("-")[1].trim());
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
            commentPs.setInt(1, questionId);
            commentPs.setInt(2, commentId);
            commentPs.setString(3, commentText);
            commentPs.setString(4, commentHtml);
            commentPs.setDate(5, Date.valueOf(commentDate));
            commentPs.setString(6, user);
            commentPs.setObject(7, userReputation);
            commentPs.executeUpdate();
        }
        commentPs.close();
    }

}

package server;

import analysis.error_codes.ErrorCodeMinHashModel;
import analysis.error_codes.ErrorCodeRegexModelKt;
import analysis.error_codes.Solution;
import com.google.gson.Gson;
import database.Database;
import javafx.util.Pair;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static j2html.TagCreator.*;
import static j2html.TagCreator.head;
import static spark.Spark.*;

public class ErrorCodeSearch {

    public static void main(String[] args) throws Exception {
        port(8082);
        staticFiles.externalLocation(new File("public").getAbsolutePath());
        final Map<String, List<Solution>> errorCodeData = ErrorCodeRegexModelKt.loadErrorCodeMap();
        final Map<String, List<Pair<String,Double>>> errorCodeCorrelations = ErrorCodeRegexModelKt.loadCorrelatedErrors();
        final Map<Integer, Integer> postsToDistinctCodesMap = ErrorCodeRegexModelKt.loadDistinctCodesPerPostMap();
        final Map<String, Double> crashProbabilities = ErrorCodeRegexModelKt.loadCrashProbabilities();
        final List<Pair<String,Integer>> errorCodes = errorCodeData.entrySet().stream()
                .map(e->new Pair<>(e.getKey(), e.getValue().size())).sorted((e1,e2)->Integer.compare(e2.getValue(), e1.getValue()))
                .collect(Collectors.toList());

        get("/ajax/:resource", (req,res)->{
            Map<String,Object> response = new HashMap<>();
            String _query = req.queryParams("q");
            if(_query!=null&&_query.trim().length()==0) {
                _query = null;
            }
            Integer page = null;
            try {
                page = Integer.valueOf(req.queryParams("page"));
            } catch(Exception e) {
                System.out.println("No page param found...");
            }
            final String query = _query;
            List<Map<String,Object>> results = errorCodes.stream().map(pair->{
                Map<String, Object> result = new HashMap<>();
                String name = pair.getKey();
                result.put("text", name + " ("+pair.getValue()+")");
                result.put("id", name);
                return result;
            }).filter(m->query==null||m.get("id").toString().toLowerCase().contains(query)).collect(Collectors.toList());

            if(page!=null) {
                int start = page*20;
                if(results.size()>start) {
                    results = results.subList(start, results.size());
                }
            }
            Map<String,Object> pagination = new HashMap<>();
            if(results.size()>20) {
                pagination.put("more", true);
                results = results.subList(0, 20);
            }
            response.put("results", results);
            response.put("pagination", pagination);
            return new Gson().toJson(response);
        });

        get("/", (req, res)->{
            req.session(true);
            return Main.htmlWrapper(div().withClass("container-fluid").with(
                div().withClass("row").attr("style", "margin-top: 5%;").with(
                        div().withClass("col-12").with(
                                h4("Error Code Search")
                        ),
                        div().withClass("col-12").with(
                                div().withClass("row").with(
                                        div().withClass("col-12 col-md-6").with(
                                                form().withClass("error_form").with(
                                                        label().attr("style", "width: 100%").with(
                                                                h5("Please enter an error code."),
                                                                select().withClass("error_search").withType("text").attr("style", "width: 100%;").withName("error_search")
                                                        ),br(),
                                                        button("Search").withClass("btn btn-outline-secondary").withType("submit")
                                                )
                                        )
                                )
                        ),div().withClass("col-12").with(
                                h5("Results"),
                                div().withId("results")
                        )
                )
            ), "Error Code Search").render();
        });


        post("/recommend", (req, res) -> {
            String errorCode = req.queryParams("error_search");
            int limit = 20;
            System.out.println("Error code: "+errorCode);
            String html;
            {
                List<Pair<Solution, Double>> topAnswers = findTopAnswers(errorCode, errorCodeData, postsToDistinctCodesMap, limit);
                List<Pair<String, Double>> correlatedErrors = errorCodeCorrelations.getOrDefault(errorCode, Collections.emptyList());
                double crashProb = crashProbabilities.getOrDefault(errorCode, 0.01) * 100;
                AtomicInteger cnt = new AtomicInteger(0);
                html = div().withClass("col-12").with(
                        div().withClass("row").with(
                                div().withClass("col-12").with(
                                        h5("Crash Probability"),
                                        b(String.format("%.2f", crashProb)+"%")
                                )
                        ),
                        div().withClass("row").with(
                                div().withClass("col-12").with(
                                        h5("Correlated Error Codes"),
                                        div().with(
                                                correlatedErrors.stream().sorted((e1,e2)->Double.compare(e2.getValue(),e1.getValue())).limit(limit).map(tag->span(tag.getKey()+" ("+String.format("%.2f",tag.getValue())+")").attr("style", "margin-right: 15px;"))
                                                        .collect(Collectors.toList())

                                        )
                                )
                        ),
                        div().withClass("row").with(
                                div().withClass("col-12").with(
                                        h5("Top Answers"),
                                        div().with(
                                                topAnswers.stream().map(top-> div().with(
                                                            div(b(String.valueOf(cnt.incrementAndGet()) + ". (Score: " + top.getValue() + ")")),
                                                            div(a("Link to Question (" + Database.selectTitleOf(top.getKey().getPostId()) + ")").attr("href", "https://stackoverflow.com/questions/" + top.getKey().getPostId() + "/")),
                                                            div().attr("style", "border: black 1px solid; padding: 10px; ").withClass("answer-body").attr("data-html", Database.selectAnswerBody(top.getKey().getAnswerId())),
                                                            hr()
                                                    )
                                                ).collect(Collectors.toList())
                                        )
                                )
                        )

                ).render();

            }
            return new Gson().toJson(Collections.singletonMap("data", html));
        });

    }

    private static List<Pair<Solution,Double>> findTopAnswers(String errorCode, Map<String, List<Solution>> data, Map<Integer, Integer> postToDistinctCountMap, int limit) {
        List<Solution> possible = data.getOrDefault(errorCode, Collections.emptyList());
        return possible.stream().map(solution -> {
            double score = ((double)solution.getOccurrences())/postToDistinctCountMap.getOrDefault(solution.getPostId(), 1);
            score *= (solution.getScore() + Math.log(solution.getViews()+Math.E));
            return new Pair<>(solution, score);
        }).sorted((e1,e2)->Double.compare(e2.getValue(), e1.getValue())).limit(limit).collect(Collectors.toList());
    }
}

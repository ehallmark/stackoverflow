package server;

import analysis.error_codes.ErrorCodesModel;
import analysis.min_hash.MinHash;
import analysis.python.PythonAdapter;
import com.google.gson.Gson;
import database.Database;
import j2html.tags.ContainerTag;
import javafx.util.Pair;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.ops.transforms.Transforms;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static j2html.TagCreator.*;
import static j2html.TagCreator.head;
import static spark.Spark.*;

public class Main {
    private static String truncateString(String str, int n) {
        if(str==null||str.length()<=n) {
            return str;
        }
        return str.substring(0, n);
    }

    public static void main(String[] args) throws Exception {
        port(8080);
        staticFiles.externalLocation(new File("public").getAbsolutePath());
        final List<Map<String,Object>> tagData = Database.loadData("tags", "name", "occurrences");
        final MinHash hash = MinHash.load(ErrorCodesModel.MIN_HASH_FILE);

        get("/ajax/:resource", (req,res)->{
            String resource = req.params("resource");
            final List<Map<String,Object>> data;
            if(resource.equals("tags")) {
                data = tagData;
            } else {
                throw new RuntimeException("Unable resource: "+resource);
            }
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
            List<Map<String,Object>> results = data.stream().map(product->{
                Map<String, Object> result = new HashMap<>();
                String name = product.get("name").toString() + "("+product.get("occurrences").toString()+")";
                result.put("text", name);
                result.put("id", String.valueOf((Integer)product.get("id")));
                return result;
            }).filter(m->query==null||m.get("text").toString().toLowerCase().contains(query)).collect(Collectors.toList());

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
            return htmlWrapper(div().withClass("container-fluid").with(
                div().withClass("row").attr("style", "margin-top: 5%;").with(
                        div().withClass("col-12").with(
                                h4("Error Code Detector")
                        ),
                        div().withClass("col-12").with(
                                div().withClass("row").with(
                                        div().withClass("col-12 col-md-6").with(
                                                form().withClass("error_form").with(
                                                        label().attr("style", "width: 100%").with(
                                                                h5("Please enter any error message, error code, or stack trace."),
                                                                textarea().attr("style", "width: 100%;").withName("error").withClass("form-control")
                                                        ),br(),
                                                        label().attr("style", "width: 100%").with(h5("Tags"),select().attr("style", "width: 100%").withName("tags[]").withClass("select_tags").attr("multiple").with(option())),
                                                        br(),
                                                        button("Search").withClass("btn btn-outline-secondary").withType("submit")
                                                )
                                        )
                                )
                        ),div().withClass("col-12").with(
                                h5("Recommendations"),
                                div().withId("results")
                        )
                )
            )).render();
        });


        post("/recommend", (req, res) -> {
            String errorStr = req.queryParams("error");
            String html;
            if((errorStr==null || errorStr.length()==0)) {
                html = "Please specify an error message, error code, or stack trace.";
            } else {
                System.out.println("Recommend questions for: " + errorStr);
                List<Pair<String, Double>> topAnswers = hash.mostSimilar(errorStr.toLowerCase(), 10);
                List<Pair<String, Double>> topTags = PythonAdapter.predictTags(errorStr.toLowerCase(), 5);
                AtomicInteger cnt = new AtomicInteger(0);
                html = div().withClass("col-12").with(
                        div().withClass("row").with(
                                div().withClass("col-12").with(
                                        h5("Relevant Tags"),
                                        div().with(
                                            topTags.stream().map(tag->span(tag+" ("+String.format("%.2f",tag.getValue())+")"))
                                            .collect(Collectors.toList())
                                        )
                                )
                        ),
                        div().withClass("row").with(
                                div().withClass("col-12 col-md-6").with(
                                        h5("Top Answers")
                                ).with(
                                        topAnswers.stream().map(top->div().with(
                                                div(b(String.valueOf(cnt.incrementAndGet())+". (Score: "+top.getValue()+")")),
                                                div(a("Link to Question").attr("href", "https://stackoverflow.com/questions/"+Database.selectParentIdOf(top.getKey())+"/")),
                                                div().attr("style", "border: black 1px solid; padding: 10px; ").withClass("answer-body").attr("data-html", Database.selectAnswerBody(top.getKey())),
                                                hr()
                                        )).collect(Collectors.toList())
                                )
                        )

                ).render();

            }
            return new Gson().toJson(Collections.singletonMap("data", html));
        });

    }


    private static ContainerTag htmlWrapper(ContainerTag inner) {
        return html().attr("lang","en").with(
                head().with(
                        meta().attr("charset","utf-8"),
                        meta().attr("http-equiv","X-UA-Compatible").attr("content","IE=edge"),
                        meta().attr("name","viewport").attr("content","width=device-width, initial-scale=1"),
                        title("Code Predictor"),
                        script().withSrc("/js/jquery-3.3.1.min.js"),
                        script().withSrc("/js/jquery-ui-1.12.1.min.js"),
                        script().withSrc("/js/popper.min.js"),
                        script().withSrc("/js/main.js"),
                        script().withSrc("/js/select2.min.js"),
                        script().withSrc("/js/bootstrap.min.js"),
                        link().withRel("stylesheet").withHref("/css/bootstrap.min.css"),
                        link().withRel("stylesheet").withHref("/css/select2.min.css"),
                        link().withRel("stylesheet").withHref("/css/jquery-ui.min.css")

                ),
                body().with(inner)
        );
    }
}

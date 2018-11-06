package analysis.word2vec;

import org.deeplearning4j.models.embeddings.learning.impl.elements.CBOW;
import org.deeplearning4j.models.embeddings.learning.impl.elements.SkipGram;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.sequencevectors.SequenceVectors;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor;
import org.deeplearning4j.text.tokenization.tokenizer.TokenPreProcess;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DiscussionsToVec {
    public static synchronized void save(LocalDateTime time, String filename, Word2Vec net) {
        if(net!=null) {
            WordVectorSerializer.writeWord2VecModel(net, new File(filename+"-"+time.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        AtomicInteger nTestsCounter = new AtomicInteger(0);
        final int saveEveryNTests = 5;
        final int minWordFrequency = 10;
        final double negativeSampling = -1;
        final double sampling = 0.0001;
        final double learningRate = 0.0001; //0.01;
        final double minLearningRate = 0.000001;// 0.00001;//0.0001;
        final int testIterations = 4000000;
        final int vectorSize = 256;
        final String modelName = "discussion_2_vec-"+vectorSize;

        final String[] words = new String[]{
                "javascript",
                "exception",
                "null",
                "void",
                "kotlin",
                "elasticsearch",
                "crash",
                "failure",
                "error",
                "unknown",
                "java",
                "c++",
                "public",
                "tree",
                "algorithm",
                "admin",
                "critical"
        };

        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);

        Word2Vec net = null;
        boolean usePreviousModel = true;
        final String rootName = "/media/ehallmark/tank/models/";
        if (usePreviousModel) {
            File folder = new File(rootName);
            if(folder.exists()) {
                   // TODO!
                List<File> files = Stream.of(folder.listFiles()).sorted(Comparator.comparing(f->LocalDateTime.parse(f.getName().replace(rootName+modelName, ""))))
                        .collect(Collectors.toList());
                if(files.size()>0) {
                    System.out.println("Using model file: "+files.get(files.size()-1).getName());
                    net = WordVectorSerializer.readWord2VecModel(files.get(files.size()-1));
                    System.out.println("Loaded.");
                }
            }
        }

        Function<SequenceVectors<VocabWord>,Void> saveFunction = sequenceVectors->{
            if(nTestsCounter.getAndIncrement()%saveEveryNTests==saveEveryNTests-1) {
                System.out.println("Saving...");
                try {
                    save(LocalDateTime.now(), rootName+modelName, (Word2Vec)sequenceVectors);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return null;
        };

        final PreparedStatement ps = conn.prepareStatement("select body from posts where body is not null");
        ps.setFetchSize(10);

        SentenceIterator iterator = new SentenceIterator() {
            private ResultSet rs;
            private String next;
            @Override
            public synchronized String nextSentence() {
                return next;
            }

            @Override
            public synchronized boolean hasNext() {
                next = null;
                try {
                    if (rs.next()) {
                        next = rs.getString(1);
                        if(next != null) {
                            next = next.replaceAll("([^a-zA-Z-0-9_ ]|-)", " $1 ");
                        }
                    }
                } catch(Exception e) {

                }
                return next!=null;
            }

            @Override
            public void reset() {
                try {
                    if(rs!=null && !rs.isClosed()) {
                        rs.close();
                    }
                    rs = ps.executeQuery();
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void finish() {
                try {
                    try {
                        if (rs != null && !rs.isClosed()) {
                            rs.close();
                        }
                    } catch(Exception e2) {
                        e2.printStackTrace();
                    }
                    ps.close();
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public SentencePreProcessor getPreProcessor() {
                throw new UnsupportedOperationException("setPreProcessor");
            }

            @Override
            public void setPreProcessor(SentencePreProcessor sentencePreProcessor) {
                throw new UnsupportedOperationException("setPreProcessor");
            }
        };
        iterator.reset();

        final TokenizerFactory tokenizerFactory = new DefaultTokenizerFactory();
        tokenizerFactory.setTokenPreProcessor(new TokenPreProcess() {
            @Override
            public String preProcess(String s) {
                return s;
            }
        });

        boolean newModel = net == null;
        Word2Vec.Builder builder = new Word2Vec.Builder()
                .seed(41)
                .batchSize(512)
                .epochs(1) // hard coded to avoid learning rate from resetting
                .windowSize(6)
                .layerSize(vectorSize)
                .sampling(sampling)
                .useVariableWindow(4,6,8)
                .negativeSample(negativeSampling)
                .learningRate(learningRate)
                .tokenizerFactory(tokenizerFactory)
                .minLearningRate(minLearningRate)
                .allowParallelTokenization(true)
                .useAdaGrad(true)
                .resetModel(newModel)
                .minWordFrequency(minWordFrequency)
                .workers(Math.max(1,Runtime.getRuntime().availableProcessors()))
                .iterations(1)
                .setVectorsListeners(Collections.singleton(new CustomWordVectorListener(saveFunction,modelName,testIterations,words)))
                .useHierarchicSoftmax(true)
                .stopWords(Collections.emptySet())
                //.trainElementsRepresentation(true)
                //.trainSequencesRepresentation(true)
                //.sequenceLearningAlgorithm(new DBOW<>())
                .elementsLearningAlgorithm(new SkipGram<>())
                .iterate(iterator);

        if(!newModel) {
            builder = builder
                    .vocabCache(net.vocab())
                    .lookupTable(net.lookupTable());
        }


        net = builder.build();
        net.fit();

        System.out.println("Saving final model...");
        saveFunction.apply(net);
        System.out.println("Saved.");
        conn.close();
    }
}

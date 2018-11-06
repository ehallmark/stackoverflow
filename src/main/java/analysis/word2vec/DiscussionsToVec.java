package analysis.word2vec;

import org.deeplearning4j.models.embeddings.learning.impl.elements.CBOW;
import org.deeplearning4j.models.embeddings.learning.impl.elements.SkipGram;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.sequencevectors.SequenceVectors;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.FileSentenceIterator;
import org.deeplearning4j.text.sentenceiterator.LineSentenceIterator;
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
import java.util.*;
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
        final int minWordFrequency = 20;
        final double negativeSampling = -1;
        final double sampling = 0.0001;
        final double learningRate = 0.01; //0.0001; 0.00001; // 0.000001
        final double minLearningRate = 0.0001; //0.00001;// 0.000001; // 0.000001
        final int testIterations = 50000000;
        final int vectorSize = 256;
        final String modelName = "discussion_2_vec-"+vectorSize;
        boolean usePreviousModel = false;

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
                "critical",
                "obama",
                "trump"
        };

        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);

        Word2Vec net = null;
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
            System.out.println("Saving...");
            save(LocalDateTime.now(), rootName+modelName, (Word2Vec)sequenceVectors);
            System.out.println("Saved.");
            return null;
        };

        final PreparedStatement ps = conn.prepareStatement("select body from posts where body is not null order by random()");
        ps.setFetchSize(10);
        Random rand = new Random(2352);
        List<LineSentenceIterator> iterators = Stream.of(WriteDiscussionsDatasetKt.getTopFolder().listFiles()).map(file->new LineSentenceIterator(file)).collect(Collectors.toList());
        SentenceIterator iterator = new SentenceIterator() {
            private List<LineSentenceIterator> stillRunning;
            @Override
            public synchronized String nextSentence() {
                return stillRunning.get(rand.nextInt(stillRunning.size())).nextSentence();
            }

            @Override
            public synchronized boolean hasNext() {
                stillRunning = stillRunning.stream().filter(iter->iter.hasNext()).collect(Collectors.toList());
                return stillRunning.size() > 0;
            }

            @Override
            public void reset() {
                for (LineSentenceIterator iterator : iterators) {
                    iterator.reset();
                }
                stillRunning = Collections.synchronizedList(new ArrayList<>(iterators));
            }

            @Override
            public void finish() {
                for (LineSentenceIterator iterator : iterators) {
                    iterator.finish();
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

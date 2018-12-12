package analysis.word2vec;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import database.Database;
import org.deeplearning4j.models.embeddings.learning.impl.elements.CBOW;
import org.deeplearning4j.models.embeddings.learning.impl.elements.SkipGram;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.paragraphvectors.ParagraphVectors;
import org.deeplearning4j.models.sequencevectors.SequenceVectors;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.*;
import org.deeplearning4j.text.tokenization.tokenizer.TokenPreProcess;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.io.*;
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

    public static Word2Vec load256Model() throws Exception {
        final String rootName = "/media/ehallmark/tank/models/";
        File folder = new File(rootName);
        final String modelName = "discussion_2_vec-256";
        if(folder.exists()) {
            // TODO!
            List<File> files = Stream.of(folder.listFiles()).sorted(Comparator.comparing(f->LocalDateTime.parse(f.getName().replace(modelName, ""))))
                    .collect(Collectors.toList());
            if(files.size()>0) {
                System.out.println("Using model file: "+files.get(files.size()-1).getName());
                return WordVectorSerializer.readWord2VecModel(files.get(files.size()-1));
            }
        }
        return null;
    }


    public static Map<String,Integer> loadWordToIndexMap() throws Exception {
        Map<String,Integer> map = new HashMap<>();
        try(CSVReader ois = new CSVReader(new BufferedReader(new FileReader(new File("/backup/data/discussion2VecIndexMap.csv"))))) {
            String[] line = null;
            ois.readNext(); // skip header
            while((line = ois.readNext()) != null) {
                map.put(line[0], Integer.valueOf(line[1]));
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void saveWordToIndexMap(Word2Vec word2Vec) throws Exception {
        System.out.println("Saving vocab map with size: "+word2Vec.getVocab().numWords());
        try(CSVWriter ois = new CSVWriter(new BufferedWriter(new FileWriter(new File("/backup/data/discussion2VecIndexMap.csv"))))) {
            ois.writeNext(new String[]{"word", "index"});
            for (VocabWord word : word2Vec.getVocab().vocabWords()) {
                ois.writeNext(new String[]{word.getLabel(), String.valueOf(word2Vec.indexOf(word.getLabel())+1)}, false);
            }
            ois.flush();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        final int minWordFrequency = 20;
        final double negativeSampling = -1;
        final int numEpochs = 3;
        final double sampling = 0.0001;
        final double learningRate = /*0.01; 0.0001;*/ 0.00001; // 0.000001
        final double minLearningRate = /*0.0001; 0.00001; */  0.000001; // 0.000001
        final int testIterations = 50000000;
        final int vectorSize = 256;
        final String modelName = "discussion_2_vec-"+vectorSize;
        boolean usePreviousModel = true;

        final String[] words = new String[]{
                "get",
                "post",
                "put",
                "delete",
                "patch",
                "kill",
                "destroy",
                "show",
                "retrieve",
                "edit",
                "change",
                "create",
                "new"
        };

        Word2Vec net = null;
        final String rootName = "/media/ehallmark/tank/models/";
        if (usePreviousModel) {
            File folder = new File(rootName);
            if(folder.exists()) {
                   // TODO!
                List<File> files = Stream.of(folder.listFiles()).sorted(Comparator.comparing(f->LocalDateTime.parse(f.getName().replace(modelName, ""))))
                        .collect(Collectors.toList());
                if(files.size()>0) {
                    System.out.println("Using model file: "+files.get(files.size()-1).getName());
                    net = WordVectorSerializer.readWord2VecModel(files.get(files.size()-1));
                    System.out.println("Loaded.");
                }
            }
        }

        final boolean writeToCSV = false;
        if(writeToCSV) {
            if(net==null) throw new IllegalStateException("Writing to csv but model does not exist!");
            Word2VecToCSV.writeToCSV(net, new File("/backup/data/discussions_to_vec_embedding_matrix.csv"));
        }

        final boolean writeVocabMap = false;
        if(writeVocabMap) {
            if(net==null) throw new IllegalStateException("Writing vocab map but model does not exist!");
            saveWordToIndexMap(net);
            System.exit(0);
        }

        final boolean testOnly = true;
        if (testOnly) {
            if(net==null) throw new IllegalStateException("Testing only but model does not exist!");
            for (String word : words) {
                INDArray vec = net.lookupTable().vector(word);
                if(vec!=null) {
                    Collection<String> lst = net.wordsNearest(vec, 20);
                    System.out.println("20 Words closest to '" + word + "': " + lst);
                }
            }
            System.exit(0);
        }


        final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        conn.setAutoCommit(false);

        Function<SequenceVectors<VocabWord>,Void> saveFunction = sequenceVectors->{
            System.out.println("Saving...");
            save(LocalDateTime.now(), rootName+modelName, (Word2Vec)sequenceVectors);
            System.out.println("Saved.");
            return null;
        };

        Random rand = new Random(2352);
        List<LineSentenceIterator> iterators = Stream.of(WriteDiscussionsDatasetKt.getTopFolder().listFiles())
                .map(file->{
                    LineSentenceIterator iter = new LineSentenceIterator(file);
                    iter.setPreProcessor(new SentencePreProcessor() {
                        @Override
                        public String preProcess(String s) {
                            return s.toLowerCase();
                        }
                    });
                    return iter;
                }).collect(Collectors.toList());
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
        tokenizerFactory.setTokenPreProcessor(new CommonPreprocessor());

        boolean newModel = net == null;
        Word2Vec.Builder builder = new Word2Vec.Builder()
                .seed(41)
                .batchSize(512)
                .epochs(numEpochs) // hard coded to avoid learning rate from resetting
                .windowSize(6)
                .layerSize(vectorSize)
                .sampling(sampling)
                .useVariableWindow(4,6,8)
                .negativeSample(negativeSampling)
                .learningRate(learningRate)
                .tokenizerFactory(tokenizerFactory)
                .minLearningRate(minLearningRate)
                .allowParallelTokenization(true)
                .limitVocabularySize(50000000)
                .enableScavenger(false)
                .useAdaGrad(true)
                .resetModel(newModel)
                .minWordFrequency(minWordFrequency)
                .workers(Math.max(1,Runtime.getRuntime().availableProcessors()-2))
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

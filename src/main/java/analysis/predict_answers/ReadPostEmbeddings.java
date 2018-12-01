package analysis.predict_answers;

import com.opencsv.CSVReader;
import javafx.util.Pair;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadPostEmbeddings {
    public static final int embeddingSize = 256;
    public static final String embeddingFile = "/backup/data/stackoverflow/post_embeddings.csv";
    public static final String idFile = "/backup/data/stackoverflow/post_embedding_ids.csv";
    public static Pair<List<Integer>, List<float[]>> loadData() {
        final List<Integer> idList = new ArrayList<>(1000000);
        List<float[]> data = new ArrayList<>(1000000);
        try (CSVReader reader = new CSVReader(new BufferedReader(new FileReader(new File(embeddingFile))));
            CSVReader idReader = new CSVReader(new BufferedReader(new FileReader(new File(idFile))))) {
            reader.readNext();
            idReader.readNext();
            String[] next = reader.readNext();
            String[] nextId = idReader.readNext();
            AtomicInteger cnt = new AtomicInteger(0);
            while(next!=null) {
                final float[] vec = new float[embeddingSize];
                for(int i = 0; i < embeddingSize; i++) {
                    vec[i] = Double.valueOf(next[1+i]).floatValue();
                    if(Math.abs(vec[i]) > 1) {
                        throw new RuntimeException("Invalid value: "+vec[i]);
                    }
                }
                idList.add(Integer.valueOf(nextId[1]));
                data.add(vec);
                next = reader.readNext();
                nextId = idReader.readNext();
                if(cnt.getAndIncrement() % 10000==9999) {
                    System.out.println("Seen: "+cnt.get());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Pair<>(idList, data);
    }

    public static void main(String[] args) {
        loadData();
    }
}

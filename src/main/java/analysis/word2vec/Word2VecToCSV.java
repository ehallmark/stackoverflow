package analysis.word2vec;

import com.opencsv.CSVWriter;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class Word2VecToCSV {
    // this class loads in a word2vec model and writes a to a CSV File
    public static void writeToCSV(Word2Vec word2Vec, File outputFile) throws IOException {
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(outputFile)));
        INDArray matrix = word2Vec.getLookupTable().getWeights();
        System.out.println("Matrix shape: "+matrix.shapeInfoToString());
        double[][] data = matrix.toDoubleMatrix();
        System.out.println("Matrix data length: "+data.length);
        for(int i = 0; i < data.length; i++) {
            writer.writeNext(DoubleStream.of(data[i]).mapToObj(d->String.valueOf(d)).toArray(s->new String[s]));
        }
        writer.flush();
        writer.close();
        System.out.println("Finished.");
    }
}

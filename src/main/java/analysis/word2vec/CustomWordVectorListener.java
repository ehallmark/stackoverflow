package analysis.word2vec;
import org.deeplearning4j.models.paragraphvectors.ParagraphVectors;
import org.deeplearning4j.models.sequencevectors.SequenceVectors;
import org.deeplearning4j.models.sequencevectors.enums.ListenerEvent;
import org.deeplearning4j.models.sequencevectors.interfaces.VectorsListener;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.Function;

public class CustomWordVectorListener implements VectorsListener<VocabWord> {
    private int lines;
    private String[] words;
    private static org.slf4j.Logger log = LoggerFactory.getLogger(CustomWordVectorListener.class);
    private long currentEpoch = 0;
    private Function<SequenceVectors<VocabWord>,Void> saveFunction;
    private String modelName;


    public CustomWordVectorListener(Function<SequenceVectors<VocabWord>,Void> saveFunction, String modelName, int lines, String... words) {
        this.lines=lines;
        this.words=words;
        this.modelName=modelName;
        this.saveFunction=saveFunction;
    }

    @Override
    public boolean validateEvent(ListenerEvent event, long argument) {
        switch (event) {
            case EPOCH: {
                if(argument!=currentEpoch) {
                    currentEpoch = argument;
                    //return true;
                }
                break;

            } case ITERATION: {
                break;

            } case LINE: {
                if(argument%lines==0) {
                    synchronized (CustomWordVectorListener.class) {
                        return true;
                    }
                }
                break;
            }
        }
        return false;
    }

    @Override
    public void processEvent(ListenerEvent event, SequenceVectors<VocabWord> sequenceVectors, long argument) {
        if (event.equals(ListenerEvent.LINE)) {
            synchronized (CustomWordVectorListener.class) {
                // Evaluate model
                String currentName = modelName + "[EPOCH " + currentEpoch + ", LINE " + argument + "]";
                System.out.println(currentName);

                for (String word : words) {
                    INDArray vec = sequenceVectors.lookupTable().vector(word);
                    if(vec!=null) {
                        Collection<String> lst = sequenceVectors.wordsNearest(vec, 10);
                        System.out.println("10 Words closest to '" + word + "': " + lst);

                        if (sequenceVectors instanceof ParagraphVectors) {
                            ParagraphVectors pv = (ParagraphVectors) sequenceVectors;
                            Collection<String> topLabels = pv.nearestLabels(vec, 10);
                            System.out.println("10 Labels closest to '" + word + "': " + topLabels);
                        }
                    }
                }
                if(saveFunction!=null) saveFunction.apply(sequenceVectors);
            }
        }
    }
}

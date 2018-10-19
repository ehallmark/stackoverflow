package analysis.min_hash;

import analysis.error_codes.ErrorCodesModel;
import javafx.util.Pair;
import lombok.NonNull;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class MinHash implements Serializable {
    private static final long serialVersionUID = 151L;
    private final int k;
    private final int[] shingleSizes;
    private transient Hash[] hashes;
    private List<Pair<String, int[]>> possibilities;
    public MinHash(final int k, final int... shingleSizes) {
        this.k=k;
        this.shingleSizes=shingleSizes;
        this.hashes = new Hash[k];
        for(int i = 0; i < k; i++) {
            hashes[i] = new Hash(i);
        }
    }

    public void save(String file) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(file))));
        oos.writeObject(this);
        oos.flush();
        oos.close();
    }

    public static MinHash load(String file) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))));
        MinHash minHash = (MinHash) ois.readObject();
        if(minHash!=null) {
            minHash.hashes = new Hash[minHash.k];
            for(int i = 0; i < minHash.k; i++) {
                minHash.hashes[i] = new Hash(i);
            }
        }
        return minHash;
    }

    public void initialize(List<Pair<String,String>> data) {
        AtomicInteger cnt = new AtomicInteger(0);
        this.possibilities = data.stream().map(e->{
            if(cnt.getAndIncrement()%1000==999) {
                System.out.println("Initialized min hash: "+cnt.get());
            }
            return new Pair<>(e.getKey(), createHashValues(e.getValue()));
        }).collect(Collectors.toList());
    }

    private int[] createHashValues(String str) {
        int[] mins = new int[k * shingleSizes.length];
        for(int i = 0; i < shingleSizes.length; i++) {
            int[] m = createHashValues(createShingles(" " +str + " ", shingleSizes[i]));
            for(int j = 0; j < k; j++) {
                mins[i*k+j] = m[j];
            }
        }
        return mins;
    }

    private double similarity(int[] h1, int[] h2) {
        int same = 0;
        for(int i = 0; i < h1.length; i++) {
            if(h1[i]==h2[i]) same++;
        }
        return ((double)same)/h1.length;
    }


    private int[] createHashValues(Set<String> shingles) {
        int[] mins = new int[k];
        Arrays.fill(mins, Integer.MAX_VALUE);
        for(String shingle : shingles) {
            for(int i = 0; i < k; i++) {
                Hash hash = hashes[i];
                int code = hash.hashCode(shingle);
                if(code < mins[i]) {
                    mins[i] = code;
                }
            }
        }
        return mins;
    }


    private Set<String> createShingles(String str, int shingleSize) {
        Set<String> shingles = new HashSet<>();
        for(int i = 0; i < str.length()-shingleSize; i++) {
            shingles.add(str.substring(i, i + shingleSize));
        }
        return shingles;
    }

    public List<Pair<String, Double>> mostSimilar(@NonNull String str, int limit) {
        if(possibilities==null) {
            throw new IllegalStateException("Must initialize min hash before using it.");
        }
        int[] code = createHashValues(str);

        return possibilities.stream().map(p->{
            return new Pair<>(p.getKey(), similarity(code, p.getValue()));
        }).sorted((p1,p2)->Double.compare(p2.getValue(), p1.getValue())).limit(limit)
                .collect(Collectors.toList());
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Loading min hash...");
        MinHash hash = MinHash.load(ErrorCodesModel.MIN_HASH_FILE);
        System.out.println("Loaded.");
        String line = "";
        //Enter data using BufferReader
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter error: ");
        while(!line.equals("STOP")) {
            if(!line.isEmpty()) {
                System.out.println("Top similar: \n\t"+String.join("\n\t", hash.mostSimilar(line.toLowerCase(), 10).stream()
                .map(p->p.getKey()+": "+p.getValue()).collect(Collectors.toList())));
                System.out.println();
                System.out.println("Enter error: ");
            }
            line = reader.readLine();
        }
        reader.close();
    }

}

class Hash {
    private final int idx;
    public Hash(int idx) {
        this.idx=idx;
    }

    public int hashCode(String str) {
        return (str+idx).hashCode();
        //return Objects.hash(str, idx);
    }
}

package analysis.min_hash;

import analysis.error_codes.ErrorCodesModel;
import javafx.util.Pair;
import lombok.NonNull;
import lombok.Setter;

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
    private List<Pair<Integer, short[]>> possibilities;
    private AtomicInteger cnt = new AtomicInteger(0);
    @Setter
    private Map<Integer, Double> validIds;
    public MinHash(final int k, final int... shingleSizes) {
        this.k=k;
        this.shingleSizes=shingleSizes;
        this.hashes = new Hash[k];
        for(int i = 0; i < k; i++) {
            hashes[i] = new Hash((short)i);
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
                minHash.hashes[i] = new Hash((short)i);
            }
        }
        ois.close();
        return minHash;
    }

    public void initialize(Pair<Integer,String> data) {
        if(this.possibilities==null) {
            this.possibilities = new LinkedList<>();
        }
        possibilities.add(new Pair<>(data.getKey(), createHashValues(data.getValue())));
        if(cnt.getAndIncrement()%1000==999) {
            System.out.println("Initialized min hash: "+cnt.get());
        }
    }

    private short[] createHashValues(String str) {
        short[] mins = new short[k * shingleSizes.length];
        for(int i = 0; i < shingleSizes.length; i++) {
            short[] m = createHashValues(createShingles(" " +str + " ", shingleSizes[i]));
            for(int j = 0; j < k; j++) {
                mins[i*k+j] = m[j];
            }
        }
        return mins;
    }

    private double similarity(short[] h1, short[] h2) {
        int same = 0;
        for(int i = 0; i < h1.length; i++) {
            if(h1[i]==h2[i]) same++;
        }
        return ((double)same)/h1.length;
    }


    private short[] createHashValues(Set<String> shingles) {
        short[] mins = new short[k];
        Arrays.fill(mins, Short.MAX_VALUE);
        for(String shingle : shingles) {
            for(int i = 0; i < k; i++) {
                Hash hash = hashes[i];
                short code = hash.hashCode(shingle);
                if(code < mins[i]) {
                    mins[i] = code;
                }
            }
        }
        return mins;
    }


    private Set<String> createShingles(String strs, int shingleSize) {
        Set<String> shingles = new HashSet<>();
        String[] strSplit = strs.toLowerCase().split("\\n");
        for(String str : strSplit) {
            if (shingleSize > 0) {
                for (int i = 3 - shingleSize; i < str.length() - 3; i++) {
                    String sub = str.substring(Math.max(0, i), Math.min(str.length(), i + shingleSize)).trim();
                    if(sub.length()>0) {
                        shingles.add(sub);
                    }
                }
            } else {
                // ngram
                for (String sub : str.split("\\s+")) {
                    sub = sub.trim();
                    if (sub.length() > 0) {
                        shingles.add(sub);
                    }
                }
            }
        }
        return shingles;
    }

    public List<Pair<Integer, Double>> mostSimilar(@NonNull String str, int limit) {
        if(possibilities==null) {
            throw new IllegalStateException("Must initialize min hash before using it.");
        }
        short[] code = createHashValues(str);
        System.out.println("Starting similarity...");
        return possibilities.stream().map(p->{
            System.out.println("P m1: "+p);
            return new Pair<>(p, validIds==null ? 1d : validIds.get(p.getKey()));
        }).filter(p->p.getValue()!=null).map(p->{
            System.out.println("P m2: "+p);
            return new Pair<>(p.getKey().getKey(), similarity(code, p.getKey().getValue()) * p.getValue());
        }).filter(p->p.getValue()>0).sorted((p1,p2)->Double.compare(p2.getValue(), p1.getValue())).limit(limit)
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
    private final String mod;
    public Hash(short idx) {
        this.mod = String.valueOf(Short.hashCode(idx));
    }

    public short hashCode(String str) {
        return (short)(Objects.hash(str, mod) % Short.MAX_VALUE);
        //return Objects.hash(str, idx);
    }
}

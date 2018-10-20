package analysis.error_codes;

public class Test {
    public static void main(String[] args) {
        // throw null pointer exception
        int[] x = null; //new int[3];
        for(int i = 0; i < 5; i++) {
            x[i] = 2;
        }
    }
}

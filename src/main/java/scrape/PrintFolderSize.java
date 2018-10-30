package scrape;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

public class PrintFolderSize {
    public static void main(String[] args) throws Exception {
        int lastNum = 0;
        int numSeconds = 30;
        while(true) {
            int num = new File("/media/ehallmark/tank/data/elastico/").listFiles().length;

            System.out.println("Num files: "+ num + ", Per second: "+new Double(num-lastNum) / (numSeconds));
            TimeUnit.MILLISECONDS.sleep(numSeconds * 1000);
            lastNum = num;

        }
    }
}

package scrape;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

public class PrintFolderSize {
    public static void main(String[] args) throws Exception {
        //final Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/beerdb?user=postgres&password=password&tcpKeepAlive=true");
        int lastNum = 0;
        int numSeconds = 10;
       // for(File file : new File("/home/ehallmark/data/beers/").listFiles()) {
       //     if(file.listFiles().length==0) {
       //         file.delete();
       //     }
       // }
        //PreparedStatement ps = conn.prepareStatement("select count(*) from beers");
        while(true) {
            int num = new File("/home/ehallmark/data/stack_overflow/").listFiles().length;
            //long num2 = Math.max(Stream.of(new File("/home/ehallmark/data/beers/").listFiles())
            //        .mapToLong(f->f.listFiles().length*25).sum() - 25, 0);

            System.out.println("Num files: "+ num + ", Per second: "+new Double(num-lastNum) / (numSeconds));
            //System.out.println("Num reviews: "+ num2);
            TimeUnit.MILLISECONDS.sleep(numSeconds * 1000);
            lastNum = num;
          //  ResultSet rs = ps.executeQuery();
          //  if(rs.next()) {
          //      int count = rs.getInt(1);
           //     System.out.println("INGESTED: "+count);
          //  }
        }
    }
}

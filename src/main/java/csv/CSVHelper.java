package csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CSVHelper {
    public static void writeToCSV(String file, List<String[]> data) throws IOException {
        System.out.println("Writing "+data.size()+" rows of csv to: "+file);
        CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(file)));
        for(String[] row : data) {
            writer.writeNext(row);
        }

        writer.flush();
        writer.close();
    }
    public static List<String[]> readFromCSV(String file) throws IOException {
        System.out.println("Reading from csv: "+file);
        CSVReader writer = new CSVReader(new BufferedReader(new FileReader(file)));
        List<String[]> data = new ArrayList<>();
        Iterator<String[]> iterator = writer.iterator();
        while(iterator.hasNext()) {
            data.add(iterator.next());
        }
        writer.close();
        return data;
    }

}

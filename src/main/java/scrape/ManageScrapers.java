package scrape;

import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ManageScrapers {

    public static void main(String[] args) throws Exception {
        File proxyFile = new File("proxies.csv");
        FileUtils.copyURLToFile(new URL("http://api.buyproxies.org/?a=showProxies&pid=107699&key=03b14c878d7bd334fee346f40fd2e4e4"), proxyFile);
        String[] proxies = FileUtils.readFileToString(proxyFile, Charset.defaultCharset()).split("\\n");
        final int numProxies = proxies.length;
        System.out.println("Found "+numProxies+" proxies.");
        final int sequential = 2;
        List<Process> running = new ArrayList<>();
        int startProxy = 0;
        int endProxy = 200;
        try {
            final List<Thread> threads = new ArrayList<>();
            for (int proxyIdx = startProxy; proxyIdx < endProxy; proxyIdx+=sequential) {
                final int _proxyIdx = proxyIdx;
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            String cmd = "java -cp target/classes:\"target/dependency/*\" -Xms300m -Xmx300m -Djdk.http.auth.tunneling.disabledSchemes=\"\" scrape.Scraper " + _proxyIdx + " " + sequential + " " + numProxies;
                            ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", cmd);
                            builder.redirectErrorStream(true);
                            Process p = builder.start();
                            running.add(p);
                            BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
                            String line;
                            while (true) {
                                line = r.readLine();
                                if (line == null) {
                                    break;
                                }
                                System.out.println("[Ouput for " + _proxyIdx + "]: " + line);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                thread.start();
                threads.add(thread);
            }
            for (Thread thread : threads) {
                thread.join();
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            for(Process p : running) {
                try {
                    if (p.isAlive()) {
                        p.destroyForcibly();
                    }
                } catch(Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
    }
}

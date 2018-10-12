package scrape;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ManageScrapers {

    public static void main(String[] args) throws Exception {
        List<Process> running = new ArrayList<>();
        try {
            final int numProxies = 50;
            final List<Thread> threads = new ArrayList<>();
            for (int proxyIdx = 0; proxyIdx < numProxies; proxyIdx++) {
                final int _proxyIdx = proxyIdx;
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            String cmd = "java -cp target/classes:\"target/dependency/*\" -Xms1024m -Xmx1024m -Djdk.http.auth.tunneling.disabledSchemes=\"\" scrape.Scraper " + _proxyIdx + " " + numProxies;
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

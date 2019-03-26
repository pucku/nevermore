package example;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Flusher {
    private LinkedBlockingQueue<String> queueToFile = new LinkedBlockingQueue<String>();

    private LinkedBlockingQueue<HashMap<String, String>> pendingResults = new LinkedBlockingQueue<>();
    private AtomicInteger count = new AtomicInteger(0);

    public void setCache(HashMap<String, String> cache) {
        this.pendingResults.add(cache);
    }

    public void process() {
        WriteToFile w = new WriteToFile();
        w.setDaemon(true);
        w.start();

        CacheToQueue c = new CacheToQueue();
        c.setDaemon(true);
        c.start();
    }

    class WriteToFile extends Thread {
        public void run() {
            while (true) {
                try {
                    System.out.println("take from queue:" + queueToFile.poll(1000, TimeUnit.SECONDS)
                            + " already take:" + count.incrementAndGet());
                    if (count.get() == 2000000)
                        System.out.println("over");

                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    class CacheToQueue extends Thread {
        public void run() {
            while (true) {
                try {
                    HashMap<String, String> cache = pendingResults
                            .take();
                    for (Entry<String, String> entry : cache.entrySet()) {
                        queueToFile.add(entry.getKey());
                    }
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
        }
    }
}

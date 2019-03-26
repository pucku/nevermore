package example;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TriggleFlush extends Thread {
	Cache cache;
	Flusher flusher;

	public TriggleFlush(Cache cache, Flusher flusher) {
		this.cache = cache;
		this.flusher = flusher;
	}

	public static void main(String[] args) {
		ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1);
		Cache cache = new Cache();
		Flusher flusher = new Flusher();
		flusher.process();
		TriggleFlush t = new TriggleFlush(cache, flusher);
		schedule.scheduleAtFixedRate(t, 0, 2, TimeUnit.SECONDS);

		SimulatePut s1 = t.new SimulatePut(cache);
		s1.start();
		SimulatePut s2 = t.new SimulatePut(cache);
		s2.start();
	}

	public void run() {
		cache.toFlusher(flusher);
	}

	class SimulatePut extends Thread {
		Cache cache;

		public SimulatePut(Cache cache) {
			this.cache = cache;
		}

		public void run() {
			Integer count = 1;
			while (count <= 1000000) {

				cache.put(String.valueOf(count) + this.getId(), String.valueOf(count));
				count++;

			}
		}
	}
}

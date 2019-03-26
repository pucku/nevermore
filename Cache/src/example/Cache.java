package example;

import java.util.HashMap;
import java.util.Map;

public class Cache {
    private HashMap<String, String> cache = new HashMap<String, String>();
    public Object lock = new Object();

    public void put(String key, String value) {
        synchronized (lock) {
            cache.put(key, value);
        }
    }

    public Object get(String key) {
        return cache.get(key);
    }

    public Map<String, String> getCacheMap() {
        return this.cache;
    }

    public void toFlusher(Flusher fluser) {
        synchronized (lock) {
            fluser.setCache(cache);
            cache = new HashMap<>();
        }
    }
}

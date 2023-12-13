package aqp;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class JoinQueryCache {

    private static JoinQueryCache instance;
    private final Map<UUID, JoinQuery> cache;

    private JoinQueryCache() {
        this.cache = new HashMap<UUID, JoinQuery>();
    }

    public static synchronized JoinQueryCache getInstance() {
        if (instance == null) {
            instance = new JoinQueryCache();
        }
        return instance;
    }

    public void put(JoinQuery query) {
        cache.put(query.getId(), query);
    }

    public void clear() {
        cache.clear();
    }

    public int size() {
        return cache.size();
    }

    public Set<Map.Entry<UUID, JoinQuery>> getAllElements() {
        return this.cache.entrySet();
    }
}

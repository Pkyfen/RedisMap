package ru.gnkoshelev.kontur.intern.redis.map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RedisMap implements Map<String,String> {
    private JedisPool jedisPool = new JedisPool();
    private final String MAP_ID = "redisMap:";
    private String mapId;

    public RedisMap(){
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.incr("mapId");
            this.mapId = MAP_ID + jedis.get("mapId");
            jedis.hincrBy("usedMap", mapId, 1);
        }
    }

    @Override
    public int size() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hlen(mapId).intValue();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        try (Jedis jedis = jedisPool.getResource()){
            return jedis.hkeys(mapId).contains(key);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        try (Jedis jedis = jedisPool.getResource()){
            return jedis.hkeys(mapId).contains(value);
        }
    }

    @Override
    public String get(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hget(mapId, (String) key);
        }
    }

    @Override
    public String put(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return String.valueOf(jedis.hset(mapId, key, value));
        }
    }

    @Override
    public String remove(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return String.valueOf(jedis.hdel(mapId, (String) key));
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
       try (Jedis jedis = jedisPool.getResource()) {
           Pipeline p = jedis.pipelined();
           m.forEach((key,value) -> {
               p.hset(mapId, key, value);
           });
           p.sync();
       }
    }

    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(mapId);
        }
    }

    @Override
    public Set<String> keySet() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hkeys(mapId);
        }
    }

    @Override
    public Collection<String> values() {
        try (Jedis jedis = jedisPool.getResource()){
            return jedis.hvals(mapId);
        }
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        try (Jedis jedis = jedisPool.getResource()){
            return new HashSet<>(jedis.hgetAll(mapId).entrySet());
        }
    }

    @Override
    protected void finalize() throws Throwable{
        System.out.println("Try to delete map");
        try (Jedis jedis = jedisPool.getResource()){
            jedis.hincrBy("usedMap", mapId, -1);

            if (jedis.hget("usedMap", mapId).equals("0")) {
                jedis.del(mapId);
                jedis.hdel("usedMap", mapId);
            }
        }finally {
            super.finalize();
        }
    }
}

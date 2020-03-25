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
    private String mapId;

    public RedisMap(){
        try (Jedis jedis = jedisPool.getResource()) {
            setMapId(jedis);
        }
    }

    public RedisMap(Map<? extends String, ? extends String> m){
        try (Jedis jedis = jedisPool.getResource()){
            setMapId(jedis);
            this.putAll(m);
        }

    }

    private void setMapId(Jedis jedis){
        Long id = jedis.incr("mapId");
        //mapId - значение из редиса, содержит количество созданных redisMap
        this.mapId = "redisMap:" + id;
        jedis.hincrBy("usedMap", mapId, 1);
        //usedMap - Hashes в редисе, где ключ это id,  а значение - количество приложений
        //использующие redisMap с данным id
    }

    public RedisMap(String mapId){
        try (Jedis jedis = jedisPool.getResource()){
            this.mapId = mapId;
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
            String oldValue = jedis.hget(mapId, key);
            jedis.hset(mapId, key, value);
            return oldValue;
        }
    }

    @Override
    public String remove(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            String oldValue = jedis.hget(mapId, (String) key);
            jedis.hdel(mapId, (String) key);
            return oldValue;
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
            Set<Entry<String, String>> entrySet = new HashSet<>();
            for(Entry<String, String> entry : jedis.hgetAll(mapId).entrySet()){
                entrySet.add(new RedisEntry(entry.getKey(), entry.getValue(), mapId));
            }
            return entrySet;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(this.size() != ((RedisMap) obj).size()) return false;

        Set<String> thisKey = keySet();
        Set<String> objKey = ((RedisMap) obj).keySet();
        if(!thisKey.containsAll(objKey)) return false;

        Collection<String> thisValue = values();
        Collection<String> objValue = ((RedisMap) obj).values();
        return thisValue.containsAll(objValue);
    }

    @Override
    protected void finalize() throws Throwable{
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

    private static class RedisEntry implements Map.Entry<String, String>{

        String key;
        String value;
        String mapId;

        RedisEntry(String key, String value, String mapId){
            this.value = value;
            this.key = key;
            this.mapId = mapId;
        }

        @Override
        public String getKey() {
            return this.key;
        }

        @Override
        public String getValue() {
            return this.value;
        }

        @Override
        public String setValue(String s) {
            Jedis jedis = new Jedis();
            String oldValue = jedis.hget(mapId, key);
            jedis.hset(mapId,key,s);
            this.value = s;
            return oldValue;
        }

        @Override
        public String toString() {
            return key + " = " + value;
        }
    }
}

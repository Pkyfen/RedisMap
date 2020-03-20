package ru.gnkoshelev.kontur.intern.redis.map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class RedisMapTest {
    Map<String, String> map1;
    Map<String, String> map2;
    Map<String, String> map3;


    @Before
    public void setUp(){
        this.map1 = new RedisMap();
        this.map2 = new RedisMap();

        map1.put("one", "1");

        map2.put("one", "ONE");
        map2.put("two", "TWO");

        this.map3 = new RedisMap("redisMap:0");
        map3.put("Name", "Denis");
        map3.put("LastName", "Podkovyrov");
        map3.put("Age", "19");
    }

    @Test
    public void baseTests() {

        Assert.assertEquals("1", map1.get("one"));
        Assert.assertEquals(1, map1.size());
        Assert.assertEquals(2, map2.size());

        map1.put("one", "first");

        Assert.assertEquals("first", map1.get("one"));
        Assert.assertEquals(1, map1.size());

        Assert.assertTrue(map1.containsKey("one"));
        Assert.assertFalse(map1.containsKey("two"));

        Set<String> keys2 = map2.keySet();
        Assert.assertEquals(2, keys2.size());
        Assert.assertTrue(keys2.contains("one"));
        Assert.assertTrue(keys2.contains("two"));

        Collection<String> values1 = map1.values();
        Assert.assertEquals(1, values1.size());
        Assert.assertTrue(values1.contains("first"));
    }

    @Test
    public void getMapFromRedisTest(){
       Map<String, String> mapFromRedis = new RedisMap("redisMap:0");
       Assert.assertEquals(map3.get("Name"), mapFromRedis.get("Name"));
       Assert.assertEquals(map3.get("LastName"), mapFromRedis.get("LastName"));
       Assert.assertEquals(map3.get("Age"), mapFromRedis.get("Age"));
    }


    @Test
    public void deleteAfterGCTest() throws Exception{
        Map<String, String> map = new RedisMap("redisMap");
        Jedis jedis = new Jedis();

        map.put("Key", "Value");
        map.put("Key2", "Value2");
        map.put("Key3", "Value3");

        Assert.assertEquals(3, map.size());

        map = null;
        System.gc();
        Thread.sleep(100);

        Assert.assertEquals(0,jedis.hlen("redisMap").intValue());
    }

    @Test
    public void deleteAfterGCWithTwoClientTest() throws InterruptedException {

        Jedis jedis = new Jedis();

        Runnable task1 = () -> {
          try{
              RedisMap redisMap = new RedisMap("redisMap:1");
              redisMap.put("Test", "1");
              Thread.sleep(200);
              redisMap = null;
              System.gc();;
              Thread.sleep(200);
          }catch (Exception e){
              e.printStackTrace();
          }
        };

        Runnable task2 = () -> {
          try {
              RedisMap redisMap = new RedisMap("redisMap:1");
              redisMap.put("Test2", "2");
              Assert.assertEquals("2", jedis.hget("usedMap", "redisMap:1") );
              redisMap = null;
              System.gc();
              Thread.sleep(100);
          }catch (Exception e){
              e.printStackTrace();
          }
        };

        new Thread(task1).start();
        new Thread(task2).start();

        Thread.sleep(1000);
        Assert.assertNull(jedis.hget("usedMap","redisMap:1"));
    }

    @Test
    public void putAllTest(){
        Map<String, String> source = new HashMap<>();
        source.put("three", "THREE");
        source.put("fourth", "FOURTH");
        source.put("five", "FIVE");
        source.put("six", "SIX");
        source.put("seven", "SEVEN");

        Assert.assertEquals(2,map2.size());
        map2.putAll(source);
        Assert.assertEquals(7,map2.size());
        System.out.println(map2.entrySet());
}

    @Test
    public void entrySetTest(){
        Set<Map.Entry<String, String >> entries = map2.entrySet();
        Assert.assertEquals(2,map2.size());

        for(Map.Entry<String, String> entry:entries){
            Assert.assertEquals(map2.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void removeTest(){
        Assert.assertEquals(2,map2.size());
        map2.remove("one");
        Assert.assertEquals(1,map2.size());
    }

    @Test
    public void isEmptyTest(){
        Assert.assertFalse(map1.isEmpty());
        map1.remove("one");
        Assert.assertTrue(map1.isEmpty());
    }

    @Test
    public void clearTest(){
        Assert.assertEquals(2,map2.size());
        map2.clear();
        Assert.assertEquals(0, map2.size());
        Assert.assertEquals(0, map2.entrySet().size());
    }

    @After
    public void shutDown() throws InterruptedException {
        this.map3 = null;
        this.map2 = null;
        this.map1 = null;

        System.gc();
        Thread.sleep(200);
    }

}

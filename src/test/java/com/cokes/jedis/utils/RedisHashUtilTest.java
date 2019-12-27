package com.cokes.jedis.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisHashUtilTest {
    @Autowired
    private RedisHashUtil redisHashUtil;

    @Test
    public void  testHSet(){
        Map<String,String> user = new HashMap<>();
        user.put("name","tom");
        user.put("age","12");
        Long hSetResult = redisHashUtil.hSet("user:1", user);
        Assert.assertEquals(1,hSetResult,10);
    }

    @Test
    public void testHSetOneFieldValue(){
        Long hSetResult = redisHashUtil.hSet("user:1", "city", "tianjin");
        Assert.assertEquals(1,hSetResult,10);
    }

    @Test
    public void testHGet() {
        String name = redisHashUtil.hGet("user:1","name");
        Assert.assertEquals("tom",name);
    }

    @Test
    public void testHDel(){
        redisHashUtil.hSet("user:1","testDel","testDel");
        Long hDelResult = redisHashUtil.hDel("user:1","testDel");
        Assert.assertEquals(1,hDelResult,10);
    }

    @Test
    public void testHLen(){
        redisHashUtil.hSet("user:1:len","testDel","testDel");
        Long fieldLen = redisHashUtil.hLen("user:1:len");
        Assert.assertEquals(1,fieldLen,10);
    }

    @Test
    public void testHMSet(){
        Map<String,String> user = new HashMap<>();
        user.put("name","Tim");
        user.put("age","28");
        user.put("city","shanghai");
        String hMSetResult = redisHashUtil.hMSet("user:2", user);
        Assert.assertEquals("OK",hMSetResult);
    }

    @Test
    public void testHMGet(){
        Map<String,String> user = new HashMap<>();
        user.put("name","Tim");
        user.put("age","28");
        user.put("city","shanghai");
        redisHashUtil.hMSet("user:3", user);
        List<String> fields = new ArrayList<>();
        fields.add("name");
        fields.add("age");
        fields.add("city");
        List<String> values = redisHashUtil.hMGet("user:3", fields.toArray(new String[fields.size()]));
        Assert.assertNotNull(values);
    }

    @Test
    public void testHExists(){
        Boolean isExists = redisHashUtil.hExists("user:4", "name");
        Assert.assertEquals(Boolean.FALSE,isExists);
    }

    @Test
    public void testHFields(){
        Set<String> fields = redisHashUtil.hFields("user:5");
        Assert.assertEquals(0,fields.size());
    }

    @Test
    public void testHValues(){
        List<String> values = redisHashUtil.hValues("user:6");
        Assert.assertEquals(0,values.size());
    }

    @Test
    public void testHGetAll(){
        Map<String, String> fieldValues = redisHashUtil.hGetAll("user:7");
        Assert.assertEquals(0,fieldValues.size());
    }

    @Test
    public void testHIncrBy(){
        Long age = redisHashUtil.hIncrBy("user:8", "age", 10L);
        Assert.assertEquals(10,age,10);
    }

    @Test
    public void testHIncrByFloat(){
        Double price = redisHashUtil.hIncrByFloat("user:9", "price", 19.9);
        Assert.assertEquals(19.9,price,20);
    }

    @Test
    public void testHStrLen(){
        Long nameLen = redisHashUtil.hStrLen("user:10", "name");
        Assert.assertEquals(0,nameLen,10);
    }
}

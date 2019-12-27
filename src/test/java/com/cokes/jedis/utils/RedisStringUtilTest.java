package com.cokes.jedis.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisStringUtilTest {
    @Autowired
    private RedisStringUtil redisStringUtil;
    @Autowired
    private RedisKeysManagerUtil keysManagerUtil;

    @Test
    public void testSet(){
        String setResult = redisStringUtil.set("hello:string", "hello string");
        Assert.assertEquals("OK",setResult);
    }

    @Test
    public void testGet(){
        String getResult = redisStringUtil.get("hello:string");
        Assert.assertEquals("hello string",getResult);
    }

    @Test
    public void testSetEx() throws InterruptedException {
        String setExResult = redisStringUtil.setEx("set:ex",1,"one second");
        Assert.assertEquals("OK",setExResult);
        Thread.sleep(1500);
        String getResult = redisStringUtil.get("set:ex");
        Assert.assertNull(getResult);
    }

    @Test
    public void testSetPx() throws InterruptedException {
        String setExResult = redisStringUtil.setPx("set:px",1000,"one second");
        Assert.assertEquals("OK",setExResult);
        Thread.sleep(1500);
        String getResult = redisStringUtil.get("set:px");
        Assert.assertNull(getResult);
    }

    @Test
    public void testSetNx(){
        keysManagerUtil.del("set:nx");
        Long setNxResult = redisStringUtil.setNx("set:nx","nx");
        Assert.assertEquals(1,setNxResult,10);
        String nx = redisStringUtil.get("set:nx");
        Assert.assertEquals("nx",nx);
    }

    @Test
    public void testMSet(){
        List<String> keysValues = new ArrayList<>();
        keysValues.add("mset:1");
        keysValues.add("value1");
        keysValues.add("mset:2");
        keysValues.add("value2");
        String msetResult = redisStringUtil.mSet(keysValues.toArray(new String[keysValues.size()]));
        Assert.assertEquals("OK",msetResult);
    }

    @Test
    public void testMGet(){
        List<String> keysValues = new ArrayList<>();
        keysValues.add("mset:2");
        keysValues.add("mset:1");
        List<String> mGetResult = redisStringUtil.mGet(keysValues.toArray(new String[keysValues.size()]));
        Assert.assertNotNull(mGetResult);
    }

    @Test
    public void testIncr(){
        keysManagerUtil.del("incr:incr");
        Long incr = redisStringUtil.incr("incr:incr");
        Assert.assertEquals(1,incr,10);
    }

    @Test
    public void testIncrBy(){
        keysManagerUtil.del("incrBy:incrBy");
        Long incrBy = redisStringUtil.incrBy("incrBy:incrBy",10L);
        Assert.assertEquals(10,incrBy,10);
    }

    @Test
    public void testIncrByFloat(){
        keysManagerUtil.del("incrByFloat:incrByFloat");
        Double incrByFloat = redisStringUtil.incrByFloat("incrByFloat:incrByFloat", 10D);
        Assert.assertEquals(10,incrByFloat,10);
    }

    @Test
    public void testDecr(){
        keysManagerUtil.del("decr:decr");
        Long decr = redisStringUtil.decr("decr:decr");
        Assert.assertEquals(-1,decr,10);
    }

    @Test
    public void testDecrBy(){
        keysManagerUtil.del("decrBy:decrBy");
        Long decrBy = redisStringUtil.decrBy("decrBy:decrBy", 10L);
        Assert.assertEquals(-10,decrBy,10);
    }

}

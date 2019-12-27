package com.cokes.jedis.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@SpringBootTest
@RunWith(SpringRunner.class)
public class RedisListUtilTest {
    @Autowired
    private RedisListUtil redisListUtil;

    @Autowired
    private RedisKeysManagerUtil redisKeysManagerUtil;

    @Test
    public void testRPush(){
        Long rPush = redisListUtil.rPush("rpush:key", "12", "13", "15");
        Assert.assertEquals(1,rPush,10);
    }


    @Test
    public void testLPush(){
        Long rPush = redisListUtil.lPush("lpush:key", "12", "13", "15");
        Assert.assertEquals(1,rPush,10);
    }

    @Test
    public void testLRang(){
        redisListUtil.rPush("lrang:key","1","2","3");
        List<String> values = redisListUtil.lRang("lrang:key", 0, -1);
        Assert.assertEquals(3,values.size());
    }

    @Test
    public void testLIndex(){
        redisListUtil.rPush("lindex:key","1","2","3");
        String value = redisListUtil.lIndex("lindex:key", 0);
        Assert.assertEquals("1",value);
    }

    @Test
    public void testLLen(){
        redisListUtil.rPush("llen:key","1","2","3");
        Long lLen = redisListUtil.lLen("llen:key");
        Assert.assertEquals(3,lLen,10);
    }

    @Test
    public void testLPop(){
        redisListUtil.rPush("lpop:key","1","2","3");
        String value = redisListUtil.lPop("lpop:key");
        Assert.assertEquals("1",value);
    }

    @Test
    public void testRPop(){
        redisListUtil.rPush("rpop:key","1","2","3");
        String value = redisListUtil.rPop("rpop:key");
        Assert.assertEquals("3",value);
    }

    @Test
    public void testLRem(){
        redisListUtil.rPush("lrem:key","1","1","2","3");
        Long count = redisListUtil.lRem("lrem:key",0,"1");
        Assert.assertEquals(2,count,10);
    }

    @Test
    public void testLTrim(){
        redisListUtil.rPush("ltrim:key","1","1","2","3");
        String result = redisListUtil.lTrim("ltrim:key",0,1);
        Assert.assertEquals("OK",result);
    }

    @Test
    public void testLSet(){
        redisListUtil.rPush("lset:key","1","1","2","3");
        String result = redisListUtil.lSet("lset:key",0,"0");
        Assert.assertEquals("OK",result);
    }

    @Test
    public void testBLPop(){
        redisKeysManagerUtil.del("blpop:key");
        redisListUtil.rPush("blpop:key","0","1","2","3");
        List<String> values = redisListUtil.bLPop(2,"blpop:key");
        Assert.assertNotNull(values);
        Assert.assertEquals("0",values.get(1));
    }

    @Test
    public void testBRPop(){
        redisKeysManagerUtil.del("brpop:key");
        redisListUtil.rPush("brpop:key","0","1","2","3");
        List<String> values = redisListUtil.bRPop(2,"brpop:key");
        Assert.assertNotNull(values);
        Assert.assertEquals("3",values.get(1));
    }
}

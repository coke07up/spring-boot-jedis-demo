package com.cokes.jedis.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Set;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisKeysManagerUtilTest {

    @Autowired
    private RedisStringUtil redisStringUtil;

    @Autowired
    private RedisKeysManagerUtil redisKeysManagerUtil;

    @Test
    public void testKeys() {
        // 只是为了demo，生产慎用
        Set<String> keys = redisKeysManagerUtil.keys("*");
        Assert.assertNotNull(keys);
        System.out.println("keys size :" + keys.size());
    }

    @Test
    public void testDbSize(){
        Long size = redisKeysManagerUtil.dbSize();
        Assert.assertNotNull(size);
        System.out.println(size);
    }

    @Test
    public void testExists() {
        boolean isExists  = redisKeysManagerUtil.exists("hello");
        Assert.assertNotNull(isExists);
        System.out.println(isExists);
    }

    @Test
    public void testDel(){
        Long del  = redisKeysManagerUtil.del("hello");
        Assert.assertNotNull(del);
        System.out.println(del);
    }

    @Test
    public void testExpire() throws InterruptedException {
        redisStringUtil.set("hello","redis");
        Long expire = redisKeysManagerUtil.expire("hello",1);
        Assert.assertEquals(1,expire.doubleValue(),10);
        Thread.sleep(1500);
        String getResult = redisStringUtil.get("hello");
        Assert.assertNull(getResult);
    }

    @Test
    public void testTtl(){
        Long notExpire = redisKeysManagerUtil.ttl("hello");
        Assert.assertEquals(-1,notExpire.doubleValue(),10);
    }

    @Test
    public void testType(){
        redisStringUtil.set("hello:type","hello type");
        String type = redisKeysManagerUtil.type("hello:type");
        Assert.assertEquals("string",type);
    }

    @Test
    public void testObjectEncoding(){
        redisStringUtil.set("object:encoding", "encoding");
        String objectEncoding = redisKeysManagerUtil.objectEncoding("object:encoding");
        Assert.assertEquals("embstr",objectEncoding);
    }
}

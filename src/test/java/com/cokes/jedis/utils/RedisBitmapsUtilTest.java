package com.cokes.jedis.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.BitOP;

@SpringBootTest
@RunWith(SpringRunner.class)
public class RedisBitmapsUtilTest {
    @Autowired
    private RedisKeysManagerUtil keysManagerUtil;

    @Autowired
    private RedisBitmapsUtil bitmapsUtil;

    @Test
    public void testSetBit(){
        keysManagerUtil.del("unique:users:2020-01-02");
        bitmapsUtil.setBit("unique:users:2020-01-02",0,"1");
        bitmapsUtil.setBit("unique:users:2020-01-02",5,"1");
        bitmapsUtil.setBit("unique:users:2020-01-02",11,"1");
        bitmapsUtil.setBit("unique:users:2020-01-02",15,"1");
        Boolean result = bitmapsUtil.setBit("unique:users:2020-01-02", 19, "1");
        Assert.assertFalse(result);
    }

    @Test
    public void testGetBit(){
        Boolean result = bitmapsUtil.getBit("unique:users:2020-01-02", 19);
        Assert.assertTrue(result);
    }

    @Test
    public void testBitCount(){
        Long count = bitmapsUtil.bitCount("unique:users:2020-01-02", 0, 19);
        Assert.assertEquals(5,count,2);
    }

    @Test
    public void testBitOp(){
        keysManagerUtil.del("unique:users:2020-01-01");
        keysManagerUtil.del("unique:users:2020-01-02");
        keysManagerUtil.del("unique:users:and:2020-01-01_02");
        bitmapsUtil.setBit("unique:users:2020-01-01",1,"1");
        bitmapsUtil.setBit("unique:users:2020-01-01",2,"1");
        bitmapsUtil.setBit("unique:users:2020-01-01",5,"1");
        bitmapsUtil.setBit("unique:users:2020-01-01",9,"1");

        bitmapsUtil.setBit("unique:users:2020-01-02",0,"1");
        bitmapsUtil.setBit("unique:users:2020-01-02",1,"1");
        bitmapsUtil.setBit("unique:users:2020-01-02",4,"1");
        bitmapsUtil.setBit("unique:users:2020-01-02",9,"1");
        Long result = bitmapsUtil.bitOp(BitOP.AND, "unique:users:and:2020-01-01_02", "unique:users:2020-01-01", "unique:users:2020-01-02");
        Assert.assertEquals(2,result,2);
    }

    @Test
    public void testBitPos() {
        Long result = bitmapsUtil.bitPos("unique:users:and:2020-01-01_02", true);
        Assert.assertEquals(1,result,1);
    }
}

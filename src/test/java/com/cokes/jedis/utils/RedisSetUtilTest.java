package com.cokes.jedis.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Set;

@SpringBootTest
@RunWith(SpringRunner.class)
public class RedisSetUtilTest {
    @Autowired
    private RedisKeysManagerUtil redisKeysManagerUtil;
    @Autowired
    private RedisSetUtil redisSetUtil;

    @Test
    public void testSAdd(){
        redisKeysManagerUtil.del("sadd:key");
        Long sAdd = redisSetUtil.sAdd("sadd:key", "1", "2", "3", "4");
        Assert.assertEquals(4,sAdd,10);
    }

    @Test
    public void testSRem(){
        redisKeysManagerUtil.del("srem:key");
        redisSetUtil.sAdd("srem:key", "1", "2", "3", "4");
        Long sRem = redisSetUtil.sRem("srem:key", "1", "2");
        Assert.assertEquals(2,sRem,10);
    }

    @Test
    public void testSCard(){
        redisKeysManagerUtil.del("scard:key");
        redisSetUtil.sAdd("scard:key", "1", "2", "3", "4");
        Long sCard = redisSetUtil.sCard("scard:key");
        Assert.assertEquals(4,sCard,10);
    }

    @Test
    public void testsIsMember() {
        Boolean isMember = redisSetUtil.sIsMember("sismember:key", "1");
        Assert.assertFalse(isMember);
    }

    @Test
    public void testsRandMember(){
        redisKeysManagerUtil.del("srandmember:key");
        redisSetUtil.sAdd("srandmember:key","1","2","3","4");
        List<String> values = redisSetUtil.sRandMember("srandmember:key", 3);
        Assert.assertEquals(3,values.size());
    }

    @Test
    public void testsPop(){
        redisKeysManagerUtil.del("spop:key");
        redisSetUtil.sAdd("spop:key","1","2","3","4");
        String value = redisSetUtil.sPop("spop:key");
        Assert.assertNotNull(value);
    }

    @Test
    public void testsMembers(){
        redisKeysManagerUtil.del("smembers:key");
        redisSetUtil.sAdd("smembers:key","1","2","3","4");
        Set<String> values = redisSetUtil.sMembers("smembers:key");
        Assert.assertEquals(4,values.size());
    }

    @Test
    public void testSInter(){
        redisKeysManagerUtil.del("sinter:1");
        redisKeysManagerUtil.del("sinter:2");
        redisSetUtil.sAdd("sinter:1","1","2","3","4");
        redisSetUtil.sAdd("sinter:2","3","4","5","6");
        Set<String> values = redisSetUtil.sInter("sinter:1", "sinter:2");
        Assert.assertEquals(2,values.size());
    }

    @Test
    public void testSUnion() {
        redisKeysManagerUtil.del("sunion:1");
        redisKeysManagerUtil.del("sunion:2");
        redisSetUtil.sAdd("sunion:1","1","2","3","4");
        redisSetUtil.sAdd("sunion:2","3","4","5","6");
        Set<String> values = redisSetUtil.sUnion("sunion:1", "sunion:2");
        Assert.assertEquals(6,values.size());
    }

    @Test
    public void testSDiff(){
        redisKeysManagerUtil.del("sdiff:1");
        redisKeysManagerUtil.del("sdiff:2");
        redisSetUtil.sAdd("sdiff:1","1","2","3","4");
        redisSetUtil.sAdd("sdiff:2","3","4","5","6");
        Set<String> values = redisSetUtil.sDiff("sdiff:1", "sdiff:2");
        Assert.assertEquals(2,values.size());
    }

    @Test
    public void testsInterStore(){
        redisKeysManagerUtil.del("sinterstore:key");
        redisKeysManagerUtil.del("sinterstore:1");
        redisKeysManagerUtil.del("sinterstore:2");
        redisSetUtil.sAdd("sinterstore:1","1","2","3","4");
        redisSetUtil.sAdd("sinterstore:2","3","4","5","6");
        Long interStore = redisSetUtil.sInterStore("sinterstore:key", "sinterstore:1","sinterstore:2");
        Assert.assertEquals(2,interStore,10);
        Long sCard = redisSetUtil.sCard("sinterstore:key");
        Assert.assertEquals(2,sCard,10);
    }

    @Test
    public void testsUnionStore(){
        redisKeysManagerUtil.del("sunionstore:key");
        redisKeysManagerUtil.del("sunionstore:1");
        redisKeysManagerUtil.del("sunionstore:2");
        redisSetUtil.sAdd("sunionstore:1","1","2","3","4");
        redisSetUtil.sAdd("sunionstore:2","3","4","5","6");
        Long sUnionStore = redisSetUtil.sUnionStore("sunionstore:key", "sunionstore:1","sunionstore:2");
        Assert.assertEquals(6,sUnionStore,10);
        Long sCard = redisSetUtil.sCard("sunionstore:key");
        Assert.assertEquals(6,sCard,10);
    }

    @Test
    public void testsDiffStore(){
        redisKeysManagerUtil.del("sdiffstore:key");
        redisKeysManagerUtil.del("sdiffstore:1");
        redisKeysManagerUtil.del("sdiffstore:2");
        redisSetUtil.sAdd("sdiffstore:1","1","2","3","4");
        redisSetUtil.sAdd("sdiffstore:2","3","4","5","6");
        Long sDiffStore = redisSetUtil.sDiffStore("sdiffstore:key", "sdiffstore:1","sdiffstore:2");
        Assert.assertEquals(2,sDiffStore,10);
        Long sCard = redisSetUtil.sCard("sdiffstore:key");
        Assert.assertEquals(2,sCard,10);
    }
}

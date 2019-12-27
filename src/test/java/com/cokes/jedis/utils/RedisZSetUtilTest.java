package com.cokes.jedis.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.params.ZAddParams;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisZSetUtilTest {
    @Autowired
    private RedisKeysManagerUtil keysManagerUtil;
    @Autowired
    private RedisZSetUtil redisZSetUtil;

    @Test
    public void testZAddOne() {
        keysManagerUtil.del("zadd:key:one");
        Long zAdd = redisZSetUtil.zAdd("zadd:key:one", 10, "1");
        Assert.assertEquals(10, zAdd, 10);
    }

    @Test
    public void testZAddTwo() {
        keysManagerUtil.del("zadd:key:two");
        Long zAdd = redisZSetUtil.zAdd("zadd:key:two", 10, "1", ZAddParams.zAddParams().nx());
        Assert.assertEquals(10, zAdd, 10);
    }

    @Test
    public void testZAddMapOne() {
        keysManagerUtil.del("zadd:key:map:one");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        Long zAdd = redisZSetUtil.zAdd("zadd:key:map:one", scoreMembers);
        Assert.assertEquals(2, zAdd, 10);
    }

    @Test
    public void testZAddMapTwo() {
        keysManagerUtil.del("zadd:key:map:two");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        Long zAdd = redisZSetUtil.zAdd("zadd:key:map:two", scoreMembers, ZAddParams.zAddParams().nx());
        Assert.assertEquals(2, zAdd, 10);
    }

    @Test
    public void testZCard() {
        keysManagerUtil.del("zcard:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        redisZSetUtil.zAdd("zcard:key", scoreMembers);
        Long zCard = redisZSetUtil.zCard("zcard:key");
        Assert.assertEquals(2, zCard, 10);
    }

    @Test
    public void testZScore() {
        keysManagerUtil.del("zscore:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        redisZSetUtil.zAdd("zscore:key", scoreMembers);
        Double zCard = redisZSetUtil.zScore("zscore:key", "2");
        Assert.assertEquals(20, zCard, 10);
    }

    @Test
    public void testZRank() {
        keysManagerUtil.del("zrank:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        redisZSetUtil.zAdd("zrank:key", scoreMembers);
        Long zRank = redisZSetUtil.zRank("zrank:key", "2");
        Assert.assertEquals(2, zRank, 10);
    }

    @Test
    public void testZRevRank() {
        keysManagerUtil.del("zrevrank:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        redisZSetUtil.zAdd("zrevrank:key", scoreMembers);
        Long zRevRank = redisZSetUtil.zRevRank("zrevrank:key", "2");
        Assert.assertEquals(1, zRevRank, 10);
    }

    @Test
    public void testZRem() {
        keysManagerUtil.del("zrem:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        redisZSetUtil.zAdd("zrem:key", scoreMembers);
        Long zRevRank = redisZSetUtil.zRem("zrem:key", "1", "2");
        Assert.assertEquals(2, zRevRank, 10);
    }

    @Test
    public void testZIncrBy() {
        keysManagerUtil.del("zincrby:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        redisZSetUtil.zAdd("zincrby:key", scoreMembers);
        Double zIncrBy = redisZSetUtil.zIncrBy("zincrby:key", 10, "1");
        Assert.assertEquals(20, zIncrBy, 10);
    }

    @Test
    public void testZRange() {
        keysManagerUtil.del("zrange:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        scoreMembers.put("3", 30D);
        scoreMembers.put("4", 40D);
        redisZSetUtil.zAdd("zrange:key", scoreMembers);
        Set<String> zRange = redisZSetUtil.zRange("zrange:key", 1, 2);
        Assert.assertNotNull(zRange);
    }

    @Test
    public void testZRangeByScore() {
        keysManagerUtil.del("zrangebyscore:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        scoreMembers.put("3", 30D);
        scoreMembers.put("4", 40D);
        redisZSetUtil.zAdd("zrangebyscore:key", scoreMembers);
        Set<String> zRange = redisZSetUtil.zRangeByScore("zrangebyscore:key", 1, 20);
        Assert.assertEquals(2, zRange.size());
    }

    @Test
    public void testZCount() {
        keysManagerUtil.del("zcount:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        scoreMembers.put("3", 30D);
        scoreMembers.put("4", 40D);
        redisZSetUtil.zAdd("zcount:key", scoreMembers);
        Long zCount = redisZSetUtil.zCount("zcount:key", 1, 20);
        Assert.assertEquals(2, zCount,10);
    }

    @Test
    public void testZRemRangeByScore (){
        keysManagerUtil.del("zremrangebyscore:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        scoreMembers.put("3", 30D);
        scoreMembers.put("4", 40D);
        redisZSetUtil.zAdd("zremrangebyscore:key", scoreMembers);
        Long zRemRangeByScore = redisZSetUtil.zRemRangeByScore("zremrangebyscore:key", 1, 20);
        Assert.assertEquals(2, zRemRangeByScore,10);
    }

    @Test
    public void testZRemRangeByRank (){
        keysManagerUtil.del("zremrangebyrank:key");
        Map<String, Double> scoreMembers = new HashMap<>();
        scoreMembers.put("1", 10D);
        scoreMembers.put("2", 20D);
        scoreMembers.put("3", 30D);
        scoreMembers.put("4", 40D);
        redisZSetUtil.zAdd("zremrangebyrank:key", scoreMembers);
        Long zRemRangeByRank = redisZSetUtil.zRemRangeByRank("zremrangebyrank:key", 1, 2);
        Assert.assertEquals(2, zRemRangeByRank,0);
    }

    @Test
    public void testZInterStore (){
        keysManagerUtil.del("zinterstore:key");
        keysManagerUtil.del("zinterstore:key:1");
        keysManagerUtil.del("zinterstore:key:2");
        Map<String, Double> scoreMembersOne = new HashMap<>();
        scoreMembersOne.put("1", 10D);
        scoreMembersOne.put("2", 20D);
        scoreMembersOne.put("3", 30D);
        scoreMembersOne.put("4", 40D);
        redisZSetUtil.zAdd("zinterstore:key:1", scoreMembersOne);
        Map<String, Double> scoreMembersTwo = new HashMap<>();
        scoreMembersTwo.put("3", 30D);
        scoreMembersTwo.put("4", 40D);
        scoreMembersTwo.put("5", 50D);
        scoreMembersTwo.put("6", 60D);
        redisZSetUtil.zAdd("zinterstore:key:2", scoreMembersTwo);
        Long zInterStore = redisZSetUtil.zInterStore("zinterstore:key", "zinterstore:key:1","zinterstore:key:2");
        Assert.assertEquals(2, zInterStore,2);
    }

    @Test
    public void testZInterStoreZParam (){
        keysManagerUtil.del("zinterstore:key");
        keysManagerUtil.del("zinterstore:key:1");
        keysManagerUtil.del("zinterstore:key:2");
        Map<String, Double> scoreMembersOne = new HashMap<>();
        scoreMembersOne.put("1", 10D);
        scoreMembersOne.put("2", 20D);
        scoreMembersOne.put("3", 30D);
        scoreMembersOne.put("4", 40D);
        redisZSetUtil.zAdd("zinterstore:key:1", scoreMembersOne);
        Map<String, Double> scoreMembersTwo = new HashMap<>();
        scoreMembersTwo.put("3", 30D);
        scoreMembersTwo.put("4", 40D);
        scoreMembersTwo.put("5", 50D);
        scoreMembersTwo.put("6", 60D);
        redisZSetUtil.zAdd("zinterstore:key:2", scoreMembersTwo);
        ZParams zParams = new ZParams();
        zParams.aggregate(ZParams.Aggregate.SUM);
        zParams.weights(1,2);
        Long zInterStore = redisZSetUtil.zInterStore("zinterstore:key",zParams, "zinterstore:key:1","zinterstore:key:2");
        Assert.assertEquals(2, zInterStore,2);
        Double score = redisZSetUtil.zScore("zinterstore:key", "3");
        Assert.assertEquals(90,score,10);
    }

    @Test
    public void testZUnionStoreZParam (){
        keysManagerUtil.del("zunionstore:key");
        keysManagerUtil.del("zunionstore:key:1");
        keysManagerUtil.del("zunionstore:key:2");
        Map<String, Double> scoreMembersOne = new HashMap<>();
        scoreMembersOne.put("1", 10D);
        scoreMembersOne.put("2", 20D);
        scoreMembersOne.put("3", 30D);
        scoreMembersOne.put("4", 40D);
        redisZSetUtil.zAdd("zunionstore:key:1", scoreMembersOne);
        Map<String, Double> scoreMembersTwo = new HashMap<>();
        scoreMembersTwo.put("3", 30D);
        scoreMembersTwo.put("4", 40D);
        scoreMembersTwo.put("5", 50D);
        scoreMembersTwo.put("6", 60D);
        redisZSetUtil.zAdd("zunionstore:key:2", scoreMembersTwo);
        ZParams zParams = new ZParams();
        zParams.aggregate(ZParams.Aggregate.SUM);
        zParams.weights(1,2);
        Long zUnionStore = redisZSetUtil.zUnionStore("zunionstore:key",zParams, "zunionstore:key:1","zunionstore:key:2");
        Assert.assertEquals(6, zUnionStore,2);
        Double score = redisZSetUtil.zScore("zunionstore:key", "1");
        Assert.assertEquals(10,score,10);
    }

    @Test
    public void testZUnionStore (){
        keysManagerUtil.del("zunionstore:key");
        keysManagerUtil.del("zunionstore:key:1");
        keysManagerUtil.del("zunionstore:key:2");
        Map<String, Double> scoreMembersOne = new HashMap<>();
        scoreMembersOne.put("1", 10D);
        scoreMembersOne.put("2", 20D);
        scoreMembersOne.put("3", 30D);
        scoreMembersOne.put("4", 40D);
        redisZSetUtil.zAdd("zunionstore:key:1", scoreMembersOne);
        Map<String, Double> scoreMembersTwo = new HashMap<>();
        scoreMembersTwo.put("3", 30D);
        scoreMembersTwo.put("4", 40D);
        scoreMembersTwo.put("5", 50D);
        scoreMembersTwo.put("6", 60D);
        redisZSetUtil.zAdd("zunionstore:key:2", scoreMembersTwo);
        Long zUnionStore = redisZSetUtil.zUnionStore("zunionstore:key", "zunionstore:key:1","zunionstore:key:2");
        Assert.assertEquals(6, zUnionStore,2);
        Double score = redisZSetUtil.zScore("zunionstore:key", "3");
        Assert.assertEquals(60,score,10);
    }
}

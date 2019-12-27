package com.cokes.jedis.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.params.ZAddParams;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.springframework.util.ObjectUtils.isEmpty;

@Component
public class RedisZSetUtil {
    @Autowired
    private JedisPool jedisPool;

    /**
     * 添加元素
     * @param key
     * @param score
     * @param member
     * @return
     */
    public Long zAdd(String key, double score, String member){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zadd(key,score,member);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 添加元素
     * @param key
     * @param score
     * @param member
     * @param params XX 必须存在，才可以设置成功，用于更新;
     *               NX 必须不存在，才可以设置成功，用于添加;
     *               CH 返回此次操作后，有序集合元素和分数变化的个数;
     * @return
     */
    public Long zAdd(String key, double score, String member, ZAddParams params) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zadd(key,score,member,params);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 添加元素
     * @param key
     * @param scoreMembers
     * @return
     */
    public Long zAdd(String key, Map<String,Double> scoreMembers) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return  jedis.zadd(key,scoreMembers);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 添加元素
     * @param key
     * @param scoreMembers
     * @param params XX 必须存在，才可以设置成功，用于更新;
     *               NX 必须不存在，才可以设置成功，用于添加;
     *               CH 返回此次操作后，有序集合元素和分数变化的个数;
     * @return
     */
    public Long zAdd(String key, Map<String,Double> scoreMembers,ZAddParams params) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return  jedis.zadd(key,scoreMembers,params);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 计算成员个数
     * @param key
     * @return
     */
    public Long zCard(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return  jedis.zcard(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 计算某个成员的分数
     * @param key
     * @param member
     * @return
     */
    public Double zScore(String key,String member){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return  jedis.zscore(key,member);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 分数低到高返回成员排名
     * @param key
     * @param member
     * @return
     */
    public Long zRank(String key, String member){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zrank(key,member);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 分数从高到低返回成员排名
     * @param key
     * @param member
     * @return
     */
    public Long zRevRank(String key, String member){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zrevrank(key,member);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 删除成员
     * @param key
     * @param members
     * @return
     */
    public Long zRem(String key, String ... members){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zrem(key,members);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 添加成员的分数
     * @param key
     * @param score
     * @return
     */
    public Double zIncrBy (String key, double score,String member) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zincrby(key,score,member);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 返回指定排名范围的成员
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public Set<String> zRange(String key,long start, long stop) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zrange(key,start,stop);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptySet();
    }

    /**
     * 返回指定分数范围的成员
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Set<String> zRangeByScore(String key, double min, double max) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zrangeByScore(key,min,max);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptySet();
    }

    /**
     * 返回指定分数范围的成员个数
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long zCount (String key, double min, double max) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zcount(key,min,max);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 删除指定分数范围的成员
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long zRemRangeByScore(String key, double min, double max) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zremrangeByScore(key,min,max);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 删除指定排序内的升序成员
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public Long zRemRangeByRank(String key, long start, long stop){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zremrangeByRank(key,start,stop);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 求集合交集 并保存结果到 destinationKey 键
     * @param destinationKey 集合交集保存到这个键
     * @param keys
     * @return
     */
    public Long zInterStore(String destinationKey, String ... keys){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zinterstore(destinationKey,keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 求集合交集 并保存结果到 destinationKey 键
     * @param destinationKey
     * @param params  weights:
     *                每个键的权重，在做交集计算时，每个键中的每个成员会将自己的分数乘以这个权重（每个键的权重默认为 1）
     *                Aggregate：
     *                计算成员交集后，分值可以按照
     *                SUM(和), MIN(最小值), MAX(最大值)做汇总
     * @param keys
     * @return
     */
    public Long zInterStore(String destinationKey, ZParams params, String ... keys){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zinterstore(destinationKey,params,keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 求集合并集 并保存结果到 destinationKey 键
     * @param destinationKey 集合交集保存到这个键
     * @param keys
     * @return
     */
    public Long zUnionStore(String destinationKey, String ... keys){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zunionstore(destinationKey,keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }


    /**
     * 求集合并集 并保存结果到 destinationKey 键
     * @param destinationKey
     * @param params  weights:
     *                每个键的权重，在做交集计算时，每个键中的每个成员会将自己的分数乘以这个权重（每个键的权重默认为 1）
     *                Aggregate：
     *                计算成员交集后，分值可以按照
     *                SUM(和), MIN(最小值), MAX(最大值)做汇总
     * @param keys
     * @return
     */
    public Long zUnionStore(String destinationKey, ZParams params, String ... keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.zunionstore(destinationKey,params,keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }
}

package com.cokes.jedis.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.springframework.util.ObjectUtils.isEmpty;

@Component
public class RedisSetUtil {
    @Autowired
    private JedisPool jedisPool;

    /**
     * 添加元素
     * @param key
     * @param elements
     * @return
     */
    public Long sAdd(String key, String ... elements) {
        Jedis jedis = null;
        try {
             jedis = jedisPool.getResource();
             if (!isEmpty(jedis))
               return jedis.sadd(key,elements);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 删除元素
     * @param key
     * @param elements
     * @return
     */
    public Long sRem(String key, String ... elements) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.srem(key,elements);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 计算元素个数
     * @param key
     * @return
     */
    public Long sCard(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.scard(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 判断元素是否在集合中
     * @param key
     * @return
     */
    public Boolean sIsMember(String key,String member) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.sismember(key,member);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Boolean.FALSE;
    }

    /**
     * 随机从集合中返回指定个数元素
     * @param key
     * @param count
     * @return
     */
    public List<String> sRandMember(String key, int count){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.srandmember(key,count);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptyList();
    }

    /**
     * 从集合中随机弹出元素
     * @param key
     * @return
     */
    public String sPop(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.spop(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }

    /**
     * 获取所有元素
     * @param key
     * @return
     */
    public Set<String> sMembers(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.smembers(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptySet();
    }

    /**
     * 求多个集合交集
     * @param keys
     * @return
     */
    public Set<String> sInter(String ... keys){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.sinter(keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptySet();
    }

    /**
     * 求多个集合的并集
     * @param keys
     * @return
     */
    public Set<String> sUnion(String ... keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.sunion(keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptySet();
    }

    /**
     * 求多个集合的差集
     * @param keys
     * @return
     */
    public Set<String> sDiff(String ... keys){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.sdiff(keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptySet();
    }

    /**
     * 将多个集合的交集保存到 destinationKey
     * @param destinationKey
     * @param keys
     * @return
     */
    public Long sInterStore(String destinationKey, String ... keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.sinterstore(destinationKey,keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 将多个集合的交集保存到 destinationKey
     * @param destinationKey
     * @param keys
     * @return
     */
    public Long sUnionStore(String destinationKey, String ... keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.sunionstore(destinationKey,keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 将多个集合的差集保存到 destinationKey
     * @param destinationKey
     * @param keys
     * @return
     */
    public Long sDiffStore(String destinationKey, String ... keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.sdiffstore(destinationKey,keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }
}

package com.cokes.jedis.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.Set;

import static org.springframework.util.ObjectUtils.isEmpty;

@Component
public class RedisKeysManagerUtil {
    @Autowired
    private JedisPool jedisPool;

    /**
     * 获取所有的键 (生产禁止使用)
     * @param pattern
     * @return
     */
    public Set<String> keys(String pattern){
        Jedis jedis = null;
        try {
             jedis = jedisPool.getResource();
             if (!isEmpty(jedis))
                return jedis.keys(pattern);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptySet();
    }

    /**
     * dbSize
     * @return
     */
    public Long dbSize(){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.dbSize();
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 判断键是否存在
     * @param key
     * @return
     */
    public boolean exists(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.exists(key);

        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return false;
    }

    /**
     * 删除键
     * @param key
     * @return
     */
    public Long del(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.del(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     *  设置键过期时间
     * @param key
     * @param seconds
     */
    public Long expire(String key,int seconds) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
               return jedis.expire(key,seconds);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 查看键过期时间
     * -1:键没有设置过期时间
     * -2:键不存在
     * @param key
     * @return
     */
    public Long ttl(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.ttl(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 值类型 （string hash list set zset）
     * @param key
     * @return
     */
    public String type(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.type(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }

    /**
     * 查询内部编码 ()
     * string (raw int embstr)
     * hash (hashtable ziplist)
     * list (linkedlist ziplist)
     * set  (hashtable intset quicklist)
     * zset (skiplist ziplist)
     * @param key
     * @return
     */
    public String objectEncoding(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.objectEncoding(key);
        } finally {
            if (!isEmpty(jedis))
            jedis.close();
        }
        return "";
    }
}

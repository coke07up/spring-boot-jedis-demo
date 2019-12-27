package com.cokes.jedis.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.List;

import static org.springframework.util.ObjectUtils.isEmpty;

/**
 * 为了方便写 demo，根据redis数据结构拆分成5个工具类
 *
 *
 * 常用的string命令
 */
@Component
public class RedisStringUtil {

    @Autowired
    private JedisPool jedisPool;

    /**
     * 设置值
     * @param key
     * @param value
     * @return
     */
    public String set(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.set(key, value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }

    /**
     * 为键值设置秒级过期时间
     * @param key
     * @param value
     * @return
     */
    public String setEx(String key,int seconds, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.setex(key,seconds,value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }

    /**
     * 为键值设置毫秒级过期时间
     * @param key
     * @param value
     * @return
     */
    public String setPx(String key,long millisSeconds, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.psetex(key,millisSeconds,value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }

    /**
     * 键值不存在，才可以设置成功，用于添加。
     * （分布式锁）
     * @param key
     * @param value
     * @return
     */
    public Long setNx(String key,String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.setnx(key, value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 批量设置值
     * @param keyValue
     * @return
     */
    public String mSet(String... keyValue ) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.mset(keyValue);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }
    /**
     * 根据键获取值
     * @param key
     * @return
     */
    public String get(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.get(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }

    /**
     * 批量获取值
     * @param keys
     * @return
     */
    public List<String> mGet(String ... keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.mget(keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptyList();
    }

    /**
     * 对值做自增
     * 值不为整数，返回错误
     * 值是整数，返回自增后的结果
     * 键不存在，按照值为0自增，返回结果为 1
     * @param key
     * @return
     */
    public Long incr(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.incr(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     *  自增指定数字
     * @param key
     * @param increment
     * @return
     */
    public Long incrBy(String key,Long increment) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.incrBy(key,increment);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 自增浮点数
     * @param key
     * @param increment
     * @return
     */
    public Double incrByFloat(String key,double increment) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.incrByFloat(key,increment);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 自减
     * @param key
     * @return
     */
    public Long decr(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.decr(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 自减去指定数字
     * @param key
     * @param increment
     * @return
     */
    public Long decrBy(String key,Long increment) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.decrBy(key,increment);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

}

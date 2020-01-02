package com.cokes.jedis.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static org.springframework.util.ObjectUtils.isEmpty;

@Component
public class RedisHyperLogLogUtil {
    @Autowired
    private JedisPool jedisPool;

    @Autowired
    private RedisKeysManagerUtil keysManagerUtil;

    /**
     * 添加
     * @param key
     * @param elements
     * @return
     */
    public Long pfAdd(String key, String ... elements) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
               return jedis.pfadd(key, elements);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 计算独立用户数
     * @param keys
     * @return
     */
    public Long pfCount(String ... keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.pfcount(keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 合并
     * @param destkey
     * @param sourcekeys
     * @return
     */
    public String pfMerge(String destkey, String... sourcekeys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.pfmerge(destkey,sourcekeys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }
}

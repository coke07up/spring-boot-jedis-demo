package com.cokes.jedis.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.springframework.util.ObjectUtils.isEmpty;

@Component
public class RedisHashUtil {
    @Autowired
    private JedisPool jedisPool;

    /**
     * 添加 fieldValues
     * @param key
     * @param fieldValues
     * @return
     */
    public Long hSet(String key, Map<String,String> fieldValues){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hset(key,fieldValues);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 添加一对 field value
     * @param key
     * @param field
     * @param value
     * @return
     */
    public Long hSet(String key, String field, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hset(key,field,value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 批量设置值
     * @param key
     * @param fieldsValues
     * @return
     */
    public String hMSet(String key,Map<String,String> fieldsValues) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hmset(key,fieldsValues);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }
    /**
     * 获取field值
     * @param key
     * @param field
     * @return
     */
    public String hGet(String key, String field){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hget(key,field);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }

    /**
     *
     * @param key
     * @param fields
     */
    public List<String> hMGet(String key, String ... fields) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hmget(key,fields);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptyList();
    }

    /**
     * 删除多个 field
     * @param key
     * @param field
     * @return
     */
    public Long hDel(String key,String ... field) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hdel(key,field);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 计算field个数
     * @param key
     * @return
     */
    public Long hLen(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hlen(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 判断field是否存在
     * @param key
     * @param field
     * @return
     */
    public Boolean hExists(String key,String field) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hexists(key,field);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 获取所有的 field
     * @param key
     * @return
     */
    public Set<String> hFields(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hkeys(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptySet();
    }

    /**
     * 获取所有 values
     * @param key
     * @return
     */
    public List<String> hValues(String key){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hvals(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptyList();
    }

    /**
     * 获取所有的 field-value
     * 如果hash元素过多，会存在阻塞redis的可能
     * @param key
     * @return
     */
    public Map<String,String> hGetAll(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hgetAll(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptyMap();
    }

    /**
     * field 自增
     * @param key
     * @param field
     * @param value
     * @return
     */
    public Long hIncrBy(String key,String field,Long value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hincrBy(key,field,value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
}

    /**
     * 自增 field 浮点数
     * @param key
     * @param field
     * @param value
     * @return
     */
    public Double hIncrByFloat(String key, String field, double value){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hincrByFloat(key,field,value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 获取filed value值的长度
     * @param key
     * @param field
     * @return
     */
    public Long hStrLen(String key,String field){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.hstrlen(key,field);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }
}

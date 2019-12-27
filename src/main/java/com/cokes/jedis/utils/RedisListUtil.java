package com.cokes.jedis.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ListPosition;

import java.util.Collections;
import java.util.List;

import static org.springframework.util.ObjectUtils.isEmpty;

@Component
public class RedisListUtil {

    @Autowired
    private JedisPool jedisPool;

    /**
     * 从右边插入元素
     * @param key
     * @param values
     * @return
     */
    public Long rPush(String key,String ... values){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.rpush(key,values);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return null;
    }

    /**
     * 从左边插入元素
     * @param key
     * @param values
     * @return
     */
    public Long lPush(String key,String ... values){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.lpush(key,values);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return null;
    }

    /**
     * 向某个元素的前或者后插入数据
     * @param key
     * @param listPosition
     * @param pivot
     * @param value
     * @return
     */
    public Long lInsert(String key,ListPosition listPosition, String pivot,String value){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.linsert(key, listPosition,pivot,value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return null;
    }


    /**
     * 从左到右获取列表的元素
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public List<String> lRang(String key,long start, long stop){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.lrange(key,start,stop);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return Collections.emptyList();
    }

    /**
     * 获取列表指定索引下标的元素
     * @param key
     * @param index
     * @return
     */
    public String lIndex(String key, long index){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.lindex(key,index);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return "";
    }

    /**
     * 获取列表长度
     * @param key
     * @return
     */
    public Long lLen(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.llen(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return null;
    }

    /**
     * 从列表的左侧弹出数据
     * @param key
     * @return
     */
    public String lPop(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.lpop(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return "";
    }

    /**
     * 从列表右侧弹出数据
     * @param key
     * @return
     */
    public String rPop(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.rpop(key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return "";
    }

    /**
     * 删除指定元素
     * @param key
     * @param count (count >  0 : 从左到右，删除最多count个元素
     *               count == 0 : 删除所有
     *               count <  0 : 从右到左，删除最多count个元素)
     * @param value
     * @return
     */
    public Long lRem(String key, long count, String value){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.lrem(key,count,value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 按照索引范围修剪列表
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public String lTrim(String key,long start, long stop){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.ltrim(key,start,stop);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }

    /**
     * 修改指定索引下标元素
     * @param key
     * @param index
     * @param value
     * @return
     */
    public String lSet(String key, long index, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.lset(key,index,value);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return "";
    }

    /**
     * 从左侧阻塞弹出
     * @param timeout
     * @param key
     * @return
     */
    public List<String> bLPop(int timeout, String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.blpop(timeout,key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptyList();
    }

    /**
     * 从右侧阻塞弹出
     * @param timeout
     * @param key
     * @return
     */
    public List<String> bRPop(int timeout, String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.brpop(timeout,key);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Collections.emptyList();
    }
}

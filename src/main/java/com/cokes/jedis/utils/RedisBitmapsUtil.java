package com.cokes.jedis.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static org.springframework.util.ObjectUtils.isEmpty;

@Component
public class RedisBitmapsUtil {
    @Autowired
    private JedisPool jedisPool;

    /**
     * 设置值
     * @param key
     * @param offset
     * @param value
     * @return
     */
    public Boolean setBit(String key, long offset ,String value){
        Jedis jedis = null;
        try {
           jedis = jedisPool.getResource();
           if (!isEmpty(jedis))
               return jedis.setbit(key,offset,value);

        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Boolean.TRUE;
    }

    /**
     * 获取值
     * @param key
     * @param offset
     * @return
     */
    public Boolean getBit(String key,long offset){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.getbit(key,offset);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return Boolean.FALSE;
    }

    /**
     * 获取bitmaps指定范围值为1的个数
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Long bitCount (String key, long start, long end) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.bitcount(key,start,end);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * bitmaps 之间的运算
     * @param op
     * @param destKey
     * @param keys
     * @return
     */
    public Long bitOp(BitOP op, String destKey, String... keys){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.bitop(op,destKey,keys);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }

    /**
     * 计算bitmaps中第一个值为targetBit的偏移量
     * @param key
     * @param targetBit
     * @return
     */
    public Long bitPos(String key,boolean targetBit){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.bitpos(key,targetBit);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();
        }
        return null;
    }
}

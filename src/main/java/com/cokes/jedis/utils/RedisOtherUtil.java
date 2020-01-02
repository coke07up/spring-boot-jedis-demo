package com.cokes.jedis.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.*;
import redis.clients.jedis.params.GeoRadiusParam;

import java.util.Collections;
import java.util.List;

import static org.springframework.util.ObjectUtils.isEmpty;

public class RedisOtherUtil {


    @Autowired
    private JedisPool jedisPool;

    /**
     * Pipeline 批量删除
     *
     * @param keys
     */
    public void mdel(List<String> keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis)) {
                Pipeline pipeline = jedis.pipelined();
                for (String key : keys)
                    pipeline.del(key);
                pipeline.sync();
            }
        } finally {
            if (!isEmpty(jedis)) {
                jedis.close();
            }
        }
    }

    /**
     * 发布订阅（发布消息）
     *
     * @param channel
     * @param message
     */
    public void publish(String channel, String message) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis)) {
                jedis.publish(channel, message);
            }
        } finally {
            if (!isEmpty(jedis)) {
                jedis.close();
            }
        }
    }

    /**
     * 发布订阅(订阅消息)
     *
     * @param jedisPubSub
     * @param channels
     */
    public void subScribe(JedisPubSub jedisPubSub, String... channels) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis)) {
                jedis.subscribe(jedisPubSub, channels);
            }
        } finally {
            if (!isEmpty(jedis)) {
                jedis.close();
            }
        }
    }

    /**
     * 添加地理位置信息
     *
     * @param key
     * @param longitude
     * @param latitude
     * @param member
     * @return
     */
    public Long geoAdd(String key, double longitude, double latitude, String member) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.geoadd(key, longitude, latitude, member);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return null;
    }

    /**
     * 获取地理位置信息
     *
     * @param key
     * @param member
     * @return
     */
    public List<GeoCoordinate> geoPos(String key, String... member) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.geopos(key, member);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return Collections.emptyList();
    }

    /**
     * 获取两个地理位置的距离
     *
     * @param key
     * @param member1
     * @param member2
     * @return
     */
    public Double geoDist(String key, String member1, String member2, GeoUnit unit) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.geodist(key, member1, member2, unit);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return null;
    }

    /**
     * 获取指定位置范围内的地理信息位置集合
     *
     * @param key
     * @param longitude
     * @param latitude
     * @param radius
     * @param unit
     * @param param
     * @return
     */
    public List<GeoRadiusResponse> geoRadius(String key, double longitude, double latitude, double radius,
                                             GeoUnit unit, GeoRadiusParam param) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.georadius(key, longitude, latitude, radius, unit, param);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return Collections.emptyList();
    }

    /**
     * 获取指定位置范围内的地理信息位置集合
     * @param key
     * @param member
     * @param radius
     * @param unit
     * @param param
     * @return
     */
    public List<GeoRadiusResponse> geoRadiusByMember(String key, String member, double radius, GeoUnit unit,
                                                     GeoRadiusParam param) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (!isEmpty(jedis))
                return jedis.georadiusByMember(key, member, radius, unit, param);
        } finally {
            if (!isEmpty(jedis))
                jedis.close();

        }
        return Collections.emptyList();
    }
}

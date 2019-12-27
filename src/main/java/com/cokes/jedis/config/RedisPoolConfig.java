package com.cokes.jedis.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisPool;

/**
 * 单节点redis配置类
 */
@Component
public class RedisPoolConfig {
    @Value("${spring.redis.client-name}")
    private String clientName;
    @Value("${spring.redis.database}")
    private int database;
    @Value("${spring.redis.host}")
    private String host;
    @Value("${spring.redis.port}")
    private int port;
    @Value("${spring.redis.password}")
    private String password;
    @Value("${spring.redis.timeout}")
    private int timeout;

    /**
     * redis连接池
     * @return
     */
    @Bean
    public JedisPool jedisPool(){
        return new JedisPool(genericObjectPoolConfig(),host,port,timeout,password,database,clientName);
    }

    /**
     * maxActive
     * redis 配置类
     * @return
     */
    private GenericObjectPoolConfig genericObjectPoolConfig(){
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        // 连接池中最大连接数
        poolConfig.setMaxTotal(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL * 5);
        // 最大空闲连接数
        poolConfig.setMaxIdle(GenericObjectPoolConfig.DEFAULT_MAX_IDLE * 3);
        // 最小空闲连接数
        poolConfig.setMinIdle(GenericObjectPoolConfig.DEFAULT_MIN_IDLE * 2);
        //是否开启jmx监控，应用开启jmx端口切jmxEnable设置为true，就可以通过jconsole
        //或者jvisualvm看到关于连接池的相关统计
        poolConfig.setJmxEnabled(true);
        // 连接池没有连接后客户端的最大等待时间单位为毫秒
        poolConfig.setMaxWaitMillis(1000);
        // 链接的最小空闲时间，达到此值后空闲链接将被移除
        poolConfig.setMinEvictableIdleTimeMillis(1000L * 60L * 30);
        // 做空闲链接检测时，每次的采样数 默认为3
        poolConfig.setNumTestsPerEvictionRun(3);
        // 向连接池借用链接时是否做链接有效性检测，无效的链接会被移除，每次借用多执行一次ping命令 默认false
        poolConfig.setTestOnBorrow(false);
        // 向连接池归还链接时是否做链接有效性检测，无效的链接会被移除，每次归还多执行一次ping命令 默认false
        poolConfig.setTestOnReturn(false);
        // 向连接池借用链接时是否做链接空闲检测，空闲超时的链接会被移除 默认false
        poolConfig.setTestWhileIdle(false);
        // 空闲链接检测周期 默认-1 不做检查
        poolConfig.setTimeBetweenEvictionRunsMillis(-1);
        // 当连接池用尽后，调用者是否等待，这个参数是和maxWaitMillis对应的，只有当此参数为true时，
        // maxWaitMillis才生效 默认 true
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
}

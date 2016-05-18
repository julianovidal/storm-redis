package com.julianovidal.storm.redis.connector;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnector {

	private static final int DEFAULT_TIMEOUT = 7000;

	private JedisPool jedisPool;

	public RedisConnector(String host, int port) {
		this(host, port, DEFAULT_TIMEOUT);
	}
	
	public RedisConnector(String host, Integer port, Integer timeout) {
		final JedisPoolConfig config = getConfig();
		jedisPool = new JedisPool(config, host, port, timeout);
	}

	private JedisPoolConfig getConfig() {
		final JedisPoolConfig config = new JedisPoolConfig();
		config.setTestOnBorrow(false);
		config.setTestOnReturn(false);
		config.setTestWhileIdle(false);
		config.setMaxTotal(1000);
		return config;
	}

	public Jedis getResource() {
		return jedisPool.getResource();
	}
}

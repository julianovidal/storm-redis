package com.julianovidal.storm.redis.common.data;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.julianovidal.storm.redis.connector.RedisConnector;

import redis.clients.jedis.Jedis;

public class RedisHandler implements Serializable {

	private static final long serialVersionUID = 5467819994338369267L;

	private static final Logger logger = LoggerFactory.getLogger(RedisHandler.class);
	
	private String queue;
	
	private int consumeRate;
	
	private RedisConnector redisConnector;
	
	public RedisHandler(RedisConnector redisConnector, String queue, int consumeRate) {
		this.queue = queue;
		this.consumeRate = consumeRate;
		this.redisConnector = redisConnector;
	}
	
	public List<String> consume() throws IOException {
		
		if(this.queue == null)
			throw new IOException("Queue name is noit defined.");
		if(consumeRate <= 0)
			consumeRate = 1;
		
		List<String> messages = new ArrayList<String>(consumeRate);
		
		Jedis jedis = null;
		
		try {
			
			jedis = redisConnector.getResource();
			
			for(int i = 0; i < consumeRate; i++) {
			
				String message = jedis.lpop(queue);
				if(message == null || message.isEmpty())
					break;
				
				messages.add(message);
			}
			
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if(jedis != null) jedis.close();
		}
		
		return messages;
	}
	
	public void produce(List<String> messages) throws IOException {
		
		if(this.queue == null)
			throw new IOException("Queue name is noit defined.");
		
		Jedis jedis = null;
		
		try {
			
			jedis = redisConnector.getResource();
			jedis.rpush(queue, messages.toArray(new String[]{}));
			
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if(jedis != null) jedis.close();
		}
	}
	
}

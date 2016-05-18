package com.julianovidal.storm.redis.spout;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.julianovidal.storm.redis.common.data.RedisHandler;
import com.julianovidal.storm.redis.connector.RedisConnector;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RedisSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 6409599188230150735L;

	private static final Logger LOGGER = Logger.getLogger(RedisSpout.class.getName());
	
	private SpoutOutputCollector collector;
	
	private final int consumeRate;
	private final String queue;
	private final Fields fieldsDeclarer;
	private RedisHandler redisHandler;
	
	public RedisSpout(int consumeRate, String queue, String... fieldsDeclarer) {
		this.consumeRate = consumeRate;
		this.queue = queue;
		this.fieldsDeclarer = fieldsDeclarer != null && fieldsDeclarer.length > 0 ? new Fields(fieldsDeclarer) : null; 
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		String redisHost = conf.containsKey("redisHost") ? (String) conf.get("redisHost") : "localhost";
		Integer redisPort = conf.containsKey("redisPort") ? Integer.parseInt(conf.get("redisPort").toString()) : 6379;
		RedisConnector redisConnector = new RedisConnector(redisHost, redisPort);
		redisHandler = new RedisHandler(redisConnector, queue, consumeRate);
	}
	
	@Override
	public void nextTuple() {
		try {
			final List<String> messages = redisHandler.consume();
			if(messages != null && !messages.isEmpty()) {
				final Values values = new Values(messages);
				this.collector.emit(values);
			}
		} catch(IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			this.collector.reportError(e);
		}
		
		Utils.sleep(500);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if(fieldsDeclarer != null) {
			declarer.declare(fieldsDeclarer);
		}
	}
}

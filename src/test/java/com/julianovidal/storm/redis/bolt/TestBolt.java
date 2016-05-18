package com.julianovidal.storm.redis.bolt;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TestBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -667521342792637217L;

	public static Logger LOG = LoggerFactory.getLogger(TestBolt.class);

	Map<String, Object> _componentConf;
	Map<String, Object> _conf;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Object value = input.getValue(0);
		if(value instanceof List) {
			List<String> messages = (List<String>) value;
			messages.stream().forEach(message -> {
				LOG.info("Received message: " + message);
			});
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}
}
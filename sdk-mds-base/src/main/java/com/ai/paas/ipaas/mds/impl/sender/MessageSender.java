package com.ai.paas.ipaas.mds.impl.sender;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.mds.IMessageSender;
import com.ai.paas.ipaas.mds.MessageClientException;
import com.ai.paas.ipaas.util.Assert;

public class MessageSender implements IMessageSender {
	private static transient final Logger logger = LoggerFactory
			.getLogger(MessageSender.class);
	private ProducerConfig cfg = null;
	private String topic = null;
	private List<Producer<String, byte[]>> producerPool = new ArrayList<>();
	private int maxProducer = 0;
	private int paritions = 0;

	public MessageSender(ProducerConfig cfg, int maxProducer, String topic) {
		this.cfg = cfg;
		this.topic = topic;
		this.maxProducer = maxProducer;
		// 初始化
		initProducerPool();
	}

	public MessageSender(ProducerConfig cfg, int maxProducer, String topic,
			int paritions) {
		this.cfg = cfg;
		this.topic = topic;
		this.maxProducer = maxProducer;
		this.paritions = paritions;
		// 初始化
		initProducerPool();
	}

	private void initProducerPool() {
		if (maxProducer <= 0) {
			maxProducer = 1;
		}
		Producer<String, byte[]> producer = null;
		for (int i = 0; i < maxProducer; i++) {
			producer = new Producer<>(cfg);
			producerPool.add(producer);
		}
	}

	private Producer<String, byte[]> getProducer() throws Exception {
		int size = producerPool.size();
		return producerPool.get(new Random().nextInt(size));
	}

	private void destoryProducer(Producer<String, byte[]> producer) {
		if (null != producer) {
			try {
				producer.close();
			} catch (Exception e) {
				logger.error("", e);
			}
		}
		producerPool.remove(producer);
		producer = new Producer<>(cfg);
		producerPool.add(producer);
	}

	private void returnResource(Producer<String, byte[]> producer)
			throws Exception {
	}

	@Override
	public void send(String msg, long partitionKey) {
		// 从连接池获取一个实例，进行发送
		Assert.notNull(msg, "The message is null!");
		Producer<String, byte[]> producer = null;
		try {
			producer = getProducer();
			KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(
					topic, String.valueOf(partitionKey),
					msg.getBytes(PaaSConstant.CHARSET_UTF8));
			producer.send(km);
		} catch (Exception e) {
			destoryProducer(producer);
			logger.error("", e);
			throw new MessageClientException("MessageSender send:", e);
		} finally {
			if (null != producer) {
				try {
					returnResource(producer);
				} catch (Exception e) {
					logger.error("Can not return massage producer to pool:"
							+ producer, e);
				}
			}
		}
	}

	@Override
	public void send(byte[] msg, long partitionKey) {
		// 从连接池获取一个实例，进行发送
		Assert.notNull(msg, "The message is null!");
		Producer<String, byte[]> producer = null;
		try {
			producer = getProducer();
			KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(
					topic, String.valueOf(partitionKey), msg);
			producer.send(km);
		} catch (Exception e) {
			destoryProducer(producer);
			logger.error("", e);
			throw new MessageClientException("MessageSender send:", e);
		} finally {
			if (null != producer) {
				try {
					returnResource(producer);
				} catch (Exception e) {
					logger.error("Can not return massage producer to pool:"
							+ producer, e);
				}
			}
		}
	}

	@Override
	public void send(String msg, long partitionKey, String key) {
		// 从连接池获取一个实例，进行发送
		Assert.notNull(msg, "The message is null!");
		Assert.notNull(key, "The message key is null!");
		Producer<String, byte[]> producer = null;
		try {
			producer = getProducer();
			KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(
					topic, key, String.valueOf(partitionKey),
					msg.getBytes(PaaSConstant.CHARSET_UTF8));
			producer.send(km);
		} catch (Exception e) {
			destoryProducer(producer);
			logger.error("", e);
			throw new MessageClientException("MessageSender send:", e);
		} finally {
			if (null != producer) {
				try {
					returnResource(producer);
				} catch (Exception e) {
					logger.error("Can not return massage producer to pool:"
							+ producer, e);
				}
			}
		}
	}

	@Override
	public void send(byte[] msg, long partitionKey, String key) {
		// 从连接池获取一个实例，进行发送
		Assert.notNull(msg, "The message is null!");
		Assert.notNull(key, "The message key is null!");
		Producer<String, byte[]> producer = null;
		try {
			producer = getProducer();
			KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(
					topic, key, String.valueOf(partitionKey), msg);
			producer.send(km);
		} catch (Exception e) {
			destoryProducer(producer);
			logger.error("", e);
			throw new MessageClientException("MessageSender send:", e);
		} finally {
			if (null != producer) {
				try {
					returnResource(producer);
				} catch (Exception e) {
					logger.error("Can not return massage producer to pool:"
							+ producer, e);
				}
			}
		}
	}

	@Override
	public int getParititions() {
		return paritions;
	}

}

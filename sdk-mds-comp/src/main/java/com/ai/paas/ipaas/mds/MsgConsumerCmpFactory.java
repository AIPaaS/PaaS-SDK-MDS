package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.ai.paas.ipaas.ccs.zookeeper.impl.ZKPoolFactory;
import com.ai.paas.ipaas.mds.impl.consumer.MessageConsumer;
import com.ai.paas.ipaas.mds.impl.consumer.client.KafkaConfig;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.ResourceUtil;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class MsgConsumerCmpFactory {
	static {
		ResourceUtil.addBundle("com.ai.paas.ipaas.mds.ipaas-message");
	}

	private MsgConsumerCmpFactory() {
		// 禁止私有化
	}

	private static Map<String, IMessageConsumer> _consumers = new ConcurrentHashMap<String, IMessageConsumer>();

	/**
	 * @param props
	 * @param topic
	 * @param msgProcessorHandler
	 * @return
	 */
	public static IMessageConsumer getClient(Properties props, String topic,
			IMsgProcessorHandler msgProcessorHandler) {
		IMessageConsumer consumer = null;
		Assert.notNull(props,
				ResourceUtil.getMessage("com.ai.paas.ipaas.msg.cfg_null"));
		Assert.notNull(topic,
				ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
		if (null != _consumers.get(topic))
			return _consumers.get(topic);
		// 这里需要将topic加上
		props.put("kafka.topic", topic);
		KafkaConfig kafkaConfig = new KafkaConfig(props);
		// 开始构建实例
		String zkAddress = props.getProperty("mds.zookeeper.hosts");
		if (zkAddress == null || zkAddress.length() == 0) {
			throw new IllegalArgumentException(
					"Can not found zookeeper hosts. [mds.zookeeper.hosts] is null ");
		}

		int timeout = Integer.parseInt(props.getProperty(
				"mds.zookeeper.timout", "60000"));

		String[] authInfoArray = null;
		String authInfo = null;
		try {
			authInfo = props.getProperty("mds.zookeeper.authInfo");

			if (authInfo != null && authInfo.length() > 0) {
				String[] authInfoTmp = authInfo.split(":");
				if (authInfoTmp != null && authInfoTmp.length == 2) {
					authInfoArray = authInfoTmp;
				}
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"Failed to convert [mds.zookeeper.authInfo]:" + authInfo);
		}

		ZKClient zkClient = null;
		try {
			zkClient = ZKPoolFactory.getZKPool(zkAddress, timeout,
					authInfoArray).getZkClient(zkAddress);
		} catch (Exception e) {
			throw new RuntimeException(
					"Failed to create zkClient during initial message consumer",
					e);
		}
		consumer = new MessageConsumer(zkClient, kafkaConfig,
				msgProcessorHandler);
		_consumers.put(topic, consumer);
		return consumer;
	}
}

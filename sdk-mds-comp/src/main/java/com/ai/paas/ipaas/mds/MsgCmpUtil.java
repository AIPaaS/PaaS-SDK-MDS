package com.ai.paas.ipaas.mds;

import java.util.Properties;

import kafka.producer.ProducerConfig;

import com.ai.paas.ipaas.mds.impl.consumer.client.DynamicBrokersReader;
import com.ai.paas.ipaas.mds.impl.consumer.client.KafkaConfig;
import com.ai.paas.ipaas.mds.impl.consumer.client.ZkState;
import com.ai.paas.ipaas.mds.impl.sender.MessageSender;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.ResourceUtil;

public class MsgCmpUtil {
	private MsgCmpUtil() {

	}

	public static void validateTopic(String topic) {
		Assert.notNull(topic,
				ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
	}

	public static IMessageSender instanceSender(Properties kafaProps,ProducerConfig cfg,
			String topic,int maxProducer) {
		IMessageSender sender = null;
		// 这里还需要取一下分区数,由于没有记录下来，直接去取
		// 获取所有分区数，还得初始化
		KafkaConfig kafkaConfig = new KafkaConfig(kafaProps);
		ZkState zkState = new ZkState(kafkaConfig);
		DynamicBrokersReader reader = new DynamicBrokersReader(kafkaConfig,
				zkState);
		int partitionCount = reader.getNumPartitions();
		zkState.close();
		reader.close();
		// 开始构建实例
		sender = new MessageSender(cfg, maxProducer, topic, partitionCount);
		return sender;
	}

}

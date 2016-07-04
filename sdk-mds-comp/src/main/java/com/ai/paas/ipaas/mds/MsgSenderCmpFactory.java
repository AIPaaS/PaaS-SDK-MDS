package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.impl.sender.MessageSender;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.ResourceUtil;

import kafka.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class MsgSenderCmpFactory {

	static {
		ResourceUtil.addBundle("com.ai.paas.ipaas.mds.ipaas-message");
	}

	private MsgSenderCmpFactory() {
		// 禁止私有化
	}

	private static Map<String, IMessageSender> _senders = new ConcurrentHashMap<String, IMessageSender>();

	public static IMessageSender getClient(Properties kafaProps, String topic) {
		IMessageSender sender = null;
		Assert.notNull(kafaProps,
				ResourceUtil.getMessage("com.ai.paas.ipaas.msg.cfg_null"));
		Assert.notNull(topic,
				ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
		if (null != _senders.get(topic)) {
			return _senders.get(topic);
		}
		// 开始构建实例
		ProducerConfig cfg = new ProducerConfig(kafaProps);
		int maxProducer = 0;
		if (null != kafaProps.get(MsgConstant.PROP_MAX_PRODUCER)) {
			maxProducer = Integer.parseInt((String) kafaProps
					.get(MsgConstant.PROP_MAX_PRODUCER));
		}
		int pNum = 0;
		if (null != kafaProps.getProperty(MsgConstant.PARTITION_NUM))
			pNum = Integer.parseInt((String) kafaProps
					.get(MsgConstant.PARTITION_NUM));
		sender = new MessageSender(cfg, maxProducer, topic, pNum);
		_senders.put(topic, sender);
		return sender;
	}
}

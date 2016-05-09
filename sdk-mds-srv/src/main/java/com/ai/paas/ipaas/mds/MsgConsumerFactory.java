package com.ai.paas.ipaas.mds;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.ai.paas.ipaas.ccs.constants.ConfigException;
import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.ai.paas.ipaas.ccs.zookeeper.impl.ZKPoolFactory;
import com.ai.paas.ipaas.mds.impl.consumer.MessageConsumer;
import com.ai.paas.ipaas.mds.impl.consumer.client.KafkaConfig;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.ResourceUtil;

public class MsgConsumerFactory {
	static {
		ResourceUtil.addBundle("com.ai.paas.ipaas.mds.ipaas-message");
	}

	private MsgConsumerFactory() {
		// 禁止私有化
	}

	private static Map<String, IMessageConsumer> consumers = new ConcurrentHashMap<String, IMessageConsumer>();
	private static Map<String, IMessageConsumer> _consumers = new ConcurrentHashMap<String, IMessageConsumer>();

	public static IMessageConsumer getClient(AuthDescriptor ad,
			IMsgProcessorHandler msgProcessorHandler) {
		IMessageConsumer consumer = null;

		MsgUtil.validate(ad);
		AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
		MsgUtil.validateAuthResult(authResult);
		List<String> children = null;
		try {
			children = CCSComponentFactory.getConfigClient(
					authResult.getConfigAddr(), authResult.getConfigUser(),
					authResult.getConfigPasswd()).listSubPath(
					MsgConstant.MSG_CONFIG_ROOT + ad.getServiceId());
		} catch (ConfigException e) {
			throw new MessageClientException(
					"MsgSenderFactory getClient error!", e);
		}
		if (null == children || children.size() <= 0) {
			throw new MessageClientException(
					"MsgSenderFactory can not get config info for:"
							+ ad.getServiceId());
		}
		String topic = children.get(0);
		MsgUtil.validateTopic(topic);
		// 认证通过后，判断是否存在已有实例，有，直接返回
		if (null != consumers.get(ad.getServiceId() + "_" + topic)) {
			consumer = consumers.get(ad.getServiceId() + "_" + topic);
			return consumer;
		}
		// 获取内部zk地址后取得该用户的kafka配置信息，返回json
		// 获取该用户申请的kafka服务配置信息
		consumer = MsgUtil.instanceConsumer(ad.getServiceId(), authResult,
				topic, msgProcessorHandler);
		consumers.put(topic, consumer);
		return consumer;
	}

	public static IMessageConsumer getClient(AuthDescriptor ad, String topic,
			IMsgProcessorHandler msgProcessorHandler) {
		IMessageConsumer consumer = null;

		MsgUtil.validate(ad);
		MsgUtil.validateTopic(topic);
		// 认证通过后，判断是否存在已有实例，有，直接返回
		if (null != consumers.get(ad.getServiceId() + "_" + topic)) {
			consumer = consumers.get(ad.getServiceId() + "_" + topic);
			return consumer;
		}
		// 传入用户描述对象，用户认证地址，服务申请号
		// 进行用户认证
		AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
		MsgUtil.validateAuthResult(authResult);

		// 获取内部zk地址后取得该用户的kafka配置信息，返回json
		// 获取该用户申请的kafka服务配置信息
		consumer = MsgUtil.instanceConsumer(ad.getServiceId(), authResult,
				topic, msgProcessorHandler);
		consumers.put(topic, consumer);
		return consumer;
	}

	public static IMessageConsumer getClient(AuthResult authResult,
			Properties props, String topic,
			IMsgProcessorHandler msgProcessorHandler) {
		IMessageConsumer consumer = null;
		Assert.notNull(props,
				ResourceUtil.getMessage("com.ai.paas.ipaas.msg.cfg_null"));
		Assert.notNull(topic,
				ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
		if (_consumers.containsKey(topic))
			return _consumers.get(topic);
		// 这里需要将topic加上
		props.put("kafka.topic", topic);
		KafkaConfig kafkaConfig = new KafkaConfig(props);
		// 开始构建实例
		ZKClient zkClient = null;
		try {
			zkClient = ZKPoolFactory.getZKPool(authResult.getConfigAddr(),
					authResult.getConfigUser(), authResult.getConfigPasswd(),
					60000).getZkClient(authResult.getConfigAddr(),
					authResult.getConfigUser());
		} catch (Exception e) {
			throw new MessageClientException("MessageConsumer init error!", e);
		}

		consumer = new MessageConsumer(zkClient, kafkaConfig,
				msgProcessorHandler);
		_consumers.put(topic, consumer);
		return consumer;
	}
}

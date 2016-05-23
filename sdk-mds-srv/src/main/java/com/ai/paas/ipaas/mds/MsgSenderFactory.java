package com.ai.paas.ipaas.mds;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import kafka.producer.ProducerConfig;

import com.ai.paas.ipaas.ccs.constants.ConfigException;
import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.mds.impl.sender.MessageSender;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.ResourceUtil;

public class MsgSenderFactory {

	static {
		ResourceUtil.addBundle("com.ai.paas.ipaas.mds.ipaas-message");
	}

	private MsgSenderFactory() {
		// 禁止私有化
	}

	private static Map<String, IMessageSender> senders = new ConcurrentHashMap<String, IMessageSender>();
	private static Map<String, IMessageSender> _senders = new ConcurrentHashMap<String, IMessageSender>();

	public static IMessageSender getClient(AuthDescriptor ad) {
		IMessageSender sender = null;
		MsgUtil.validate(ad);
		// 进行用户认证
		AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
		MsgUtil.validateAuthResult(authResult);
		// 获取topic
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
		if (null != senders.get(topic)) {
			sender = senders.get(topic);
			return sender;
		}
	
		sender = MsgUtil.instanceSender(ad.getServiceId(), authResult, topic);
		senders.put(topic, sender);
		return sender;
	}

	public static IMessageSender getClient(AuthDescriptor ad, String topic) {
		IMessageSender sender = null;
		MsgUtil.validate(ad);
		MsgUtil.validateTopic(topic);
		// 认证通过后，判断是否存在已有实例，有，直接返回
		if (null != senders.get(topic)) {
			sender = senders.get(topic);
			return sender;
		}
		// 传入用户描述对象，用户认证地址，服务申请号
		// 进行用户认证
		AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
		MsgUtil.validateAuthResult(authResult);
		// 获取内部zk地址后取得该用户的kafka配置信息，返回json
		// 获取该用户申请的kafka服务配置信息
		sender = MsgUtil.instanceSender(ad.getServiceId(), authResult, topic);
		senders.put(topic, sender);
		return sender;
	}

	public static IMessageSender getClient(Properties kafaProps, String userId,
			String topic) {
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
		sender = new MessageSender(cfg, maxProducer, topic);
		_senders.put(topic, sender);
		return sender;
	}

}

package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.ccs.constants.ConfigException;
import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.mds.impl.sender.MessageSender;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.ResourceUtil;
import com.google.gson.Gson;

import kafka.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class MsgSenderFactory {

	static {
		ResourceUtil.addBundle("com.ai.paas.ipaas.mds.ipaas-message");
	}

	private MsgSenderFactory() {
		// 禁止私有化
	}

	private static Map<String, IMessageSender> senders = new ConcurrentHashMap<String, IMessageSender>();
	private static Map<String, IMessageSender> _senders = new ConcurrentHashMap<String, IMessageSender>();

	public static IMessageSender getClient(AuthDescriptor ad, String topic) {
		IMessageSender sender = null;

		Assert.notNull(ad,
				ResourceUtil.getMessage("com.ai.paas.ipaas.common.auth_null"));
		Assert.notNull(ad.getAuthAdress(), ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.auth_addr_null"));
		Assert.notNull(ad.getPid(), ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.auth_pid_null"));
		Assert.notNull(ad.getPassword(), ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.auth_passwd_null"));
		Assert.notNull(ad.getServiceId(),
				ResourceUtil.getMessage("com.ai.paas.ipaas.common.srvid_null"));
		Assert.notNull(topic,
				ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));

		// 认证通过后，判断是否存在已有实例，有，直接返回
		if (senders.containsKey(topic)) {
			sender = senders.get(topic);
			return sender;
		}
		// 传入用户描述对象，用户认证地址，服务申请号
		// 进行用户认证
		AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
		Assert.notNull(authResult, ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.auth_result_null"));

		// 开始初始化
		Assert.notNull(authResult.getConfigAddr(), ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.zk_addr_null"));
		Assert.notNull(authResult.getConfigUser(), ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.zk_user_null"));
		Assert.notNull(authResult.getConfigPasswd(), ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.zk_passwd_null"));

		// 获取内部zk地址后取得该用户的kafka配置信息，返回json
		// 获取该用户申请的kafka服务配置信息
		String msgConf;
		try {
			msgConf = CCSComponentFactory.getConfigClient(
					authResult.getConfigAddr(), authResult.getConfigUser(),
					authResult.getConfigPasswd()).get(
					MsgConstant.MSG_CONFIG_ROOT + ad.getServiceId()
							+ PaaSConstant.UNIX_SEPERATOR + topic + "/sender");
		} catch (ConfigException e) {
			throw new MessageClientException(
					"MsgSenderFactory getClient error!", e);
		}
		// 封装成配置对象
		ProducerConfig cfg = null;
		Gson gson = new Gson();
		Properties props = gson.fromJson(msgConf, Properties.class);
		cfg = new ProducerConfig(props);
		int maxProducer = 0;
		if (null != props.get(MsgConstant.PROP_MAX_PRODUCER)) {
			maxProducer = Integer.parseInt((String) props
					.get(MsgConstant.PROP_MAX_PRODUCER));
		}
		// 开始构建实例
		sender = new MessageSender(cfg, maxProducer, topic);
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

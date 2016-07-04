package com.ai.paas.ipaas.mds;

import java.util.Properties;

import kafka.producer.ProducerConfig;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.ccs.constants.ConfigException;
import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.ccs.inner.constants.ConfigPathMode;
import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.ai.paas.ipaas.ccs.zookeeper.impl.ZKPoolFactory;
import com.ai.paas.ipaas.mds.impl.consumer.MessageConsumer;
import com.ai.paas.ipaas.mds.impl.consumer.client.Config;
import com.ai.paas.ipaas.mds.impl.consumer.client.KafkaConfig;
import com.ai.paas.ipaas.mds.impl.sender.MessageSender;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.ResourceUtil;
import com.google.gson.Gson;

public class MsgUtil {
	private MsgUtil() {

	}

	public static void validate(AuthDescriptor ad) {
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

	}

	public static void validateTopic(String topic) {
		Assert.notNull(topic,
				ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
	}

	public static void validateAuthResult(AuthResult authResult) {
		Assert.notNull(authResult, ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.auth_result_null"));

		// 开始初始化
		Assert.notNull(authResult.getConfigAddr(), ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.zk_addr_null"));
		Assert.notNull(authResult.getConfigUser(), ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.zk_user_null"));
		Assert.notNull(authResult.getConfigPasswd(), ResourceUtil
				.getMessage("com.ai.paas.ipaas.common.zk_passwd_null"));
	}

	public static IMessageSender instanceSender(String serviceId,
			AuthResult authResult, String topic) {
		IMessageSender sender = null;
		String msgConf;
		try {
			msgConf = CCSComponentFactory.getConfigClient(
					authResult.getConfigAddr(), authResult.getConfigUser(),
					authResult.getConfigPasswd()).get(
					MsgConstant.MSG_CONFIG_ROOT + serviceId
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
		int pNum = 0;
		if (null != props.getProperty(MsgConstant.PARTITION_NUM))
			pNum = Integer.parseInt((String) props
					.get(MsgConstant.PARTITION_NUM));
		// 开始构建实例
		sender = new MessageSender(cfg, maxProducer, topic, pNum);
		return sender;
	}

	public static IMessageConsumer instanceConsumer(String serviceId,
			String consumerId, AuthResult authResult, String topic,
			IMsgProcessorHandler msgProcessorHandler) {
		IMessageConsumer consumer = null;
		KafkaConfig kafkaConfig = buildConfig(authResult, topic, serviceId,
				consumerId);
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

		return consumer;
	}

	private static KafkaConfig buildConfig(AuthResult authResult, String topic,
			String serviceId, String consumerId) {
		String msgConf;
		try {
			msgConf = CCSComponentFactory
					.getConfigClient(authResult.getConfigAddr(),
							authResult.getConfigUser(),
							authResult.getConfigPasswd())
					.get(MsgConstant.MSG_CONFIG_ROOT + serviceId
							+ PaaSConstant.UNIX_SEPERATOR + topic + "/consumer");
		} catch (ConfigException e) {
			throw new MessageClientException(
					"MsgConsumerFactory getClient error!", e);
		}
		// 封装成配置对象
		KafkaConfig kafkaConfig = null;
		Gson gson = new Gson();
		Properties props = gson.fromJson(msgConf, Properties.class);
		// 这里需要将topic加上
		props.put("kafka.topic", topic);
		props.put(Config.MDS_USER_SRV_ID, serviceId);
		String basePath = MsgConstant.MSG_CONFIG_ROOT
				+ props.getProperty(Config.MDS_USER_SRV_ID)
				+ PaaSConstant.UNIX_SEPERATOR
				+ props.getProperty("kafka.topic")
				+ PaaSConstant.UNIX_SEPERATOR + consumerId
				+ PaaSConstant.UNIX_SEPERATOR;
		try {
			// mds.partition.runninglock.path
			props.put(Config.MDS_PARTITION_RUNNING_LOCK_PATH, ConfigPathMode
					.appendPath(authResult.getConfigUser(),
							ConfigPathMode.WRITABLE.getFlag(), basePath
									+ "partitions/running"));

			// mds.partition.pauselock.path
			props.put(Config.MDS_PARTITION_PAUSE_LOCK_PATH, ConfigPathMode
					.appendPath(authResult.getConfigUser(),
							ConfigPathMode.WRITABLE.getFlag(), basePath
									+ "partitions/pause"));

			// mds.partition.offset.basepath
			props.put(Config.MDS_PARTITION_OFFSET_BASE_PATH, ConfigPathMode
					.appendPath(authResult.getConfigUser(),
							ConfigPathMode.WRITABLE.getFlag(), basePath
									+ "offsets"));

			// mds.consumer.base.path
			props.put(Config.MDS_CONSUMER_BASE_PATH, ConfigPathMode.appendPath(
					authResult.getConfigUser(),
					ConfigPathMode.WRITABLE.getFlag(), basePath + "consumers"));
		} catch (Exception e) {
			throw new MessageClientException("MessageConsumer init error!", e);
		}
		// 再次初始化
		kafkaConfig = new KafkaConfig(props);
		return kafkaConfig;
	}

	public static IMessageConsumer instanceConsumer(String serviceId,
			AuthResult authResult, String topic,
			IMsgProcessorHandler msgProcessorHandler) {
		return instanceConsumer(serviceId, "consumer", authResult, topic,
				msgProcessorHandler);
	}
}

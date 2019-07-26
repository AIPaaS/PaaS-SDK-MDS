package com.ai.paas.ipaas.mds;

import java.util.Properties;

import com.ai.paas.Constant;
import com.ai.paas.ipaas.ccs.constants.ConfigException;
import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.mds.impl.consumer.MsgConsumer;
import com.ai.paas.ipaas.mds.impl.sender.MsgProducer;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.util.Assert;
import com.ai.paas.util.JsonUtil;
import com.ai.paas.util.ResourceUtil;

public class MsgUtil {
    private MsgUtil() {

    }

    public static void validate(AuthDescriptor ad) {
        Assert.notNull(ad, ResourceUtil.getMessage("com.ai.paas.ipaas.common.auth_null"));
        Assert.notNull(ad.getAuthAdress(), ResourceUtil.getMessage("com.ai.paas.ipaas.common.auth_addr_null"));
        Assert.notNull(ad.getPid(), ResourceUtil.getMessage("com.ai.paas.ipaas.common.auth_pid_null"));
        Assert.notNull(ad.getPassword(), ResourceUtil.getMessage("com.ai.paas.ipaas.common.auth_passwd_null"));
        Assert.notNull(ad.getServiceId(), ResourceUtil.getMessage("com.ai.paas.ipaas.common.srvid_null"));

    }

    public static void validateTopic(String topic) {
        Assert.notNull(topic, ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
    }

    public static void validateAuthResult(AuthResult authResult) {
        Assert.notNull(authResult, ResourceUtil.getMessage("com.ai.paas.ipaas.common.auth_result_null"));

        // 开始初始化
        Assert.notNull(authResult.getConfigAddr(), ResourceUtil.getMessage("com.ai.paas.ipaas.common.zk_addr_null"));
        Assert.notNull(authResult.getConfigUser(), ResourceUtil.getMessage("com.ai.paas.ipaas.common.zk_user_null"));
        Assert.notNull(authResult.getConfigPasswd(),
                ResourceUtil.getMessage("com.ai.paas.ipaas.common.zk_passwd_null"));
    }

    public static IProducer instanceSender(String serviceId, AuthResult authResult, String topic) {
        IProducer sender = null;
        String msgConf;
        try {
            msgConf = CCSComponentFactory
                    .getConfigClient(authResult.getConfigAddr(), authResult.getConfigUser(),
                            authResult.getConfigPasswd())
                    .get(MsgConstant.MSG_CONFIG_ROOT + serviceId + Constant.UNIX_SEPERATOR + topic + "/sender");
        } catch (ConfigException e) {
            throw new MessageException("MsgSenderFactory getClient error!", e);
        }
        // 封装成配置对象
        Properties props = JsonUtil.fromJson(msgConf, Properties.class);
        int maxProducer = 0;
        if (null != props.get(MsgConstant.PROP_MAX_PRODUCER)) {
            maxProducer = Integer.parseInt((String) props.get(MsgConstant.PROP_MAX_PRODUCER));
        }
        // 开始构建实例
        sender = new MsgProducer(props, maxProducer, topic);
        return sender;
    }

    public static IConsumer instanceConsumer(String serviceId, String consumerId, AuthResult authResult, String topic,
            Processor processor) {
        IConsumer consumer = null;
        Properties props = buildConfig(authResult, topic, serviceId, consumerId);
        int partitions = 0;
        if (null != props.get(MsgConstant.PARTITION_NUM)) {
            partitions = Integer.parseInt((String) props.get(MsgConstant.PARTITION_NUM));
        }
        // 开始构建实例
        consumer = new MsgConsumer(props, topic, partitions, processor);

        return consumer;
    }

    private static Properties buildConfig(AuthResult authResult, String topic, String serviceId, String consumerId) {
        String msgConf;
        try {
            msgConf = CCSComponentFactory
                    .getConfigClient(authResult.getConfigAddr(), authResult.getConfigUser(),
                            authResult.getConfigPasswd())
                    .get(MsgConstant.MSG_CONFIG_ROOT + serviceId + Constant.UNIX_SEPERATOR + topic + "/consumer");
        } catch (ConfigException e) {
            throw new MessageException("MsgConsumerFactory getClient error!", e);
        }
        // 封装成配置对象
        return JsonUtil.fromJson(msgConf, Properties.class);
    }

    public static IConsumer instanceConsumer(String serviceId, AuthResult authResult, String topic,
            Processor processor) {
        return instanceConsumer(serviceId, "consumer", authResult, topic, processor);
    }
}

package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.ccs.constants.ConfigException;
import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.ccs.inner.constants.ConfigPathMode;
import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.ai.paas.ipaas.ccs.zookeeper.impl.ZKPoolFactory;
import com.ai.paas.ipaas.mds.impl.consumer.MessageConsumer;
import com.ai.paas.ipaas.mds.impl.consumer.client.Config;
import com.ai.paas.ipaas.mds.impl.consumer.client.KafkaConfig;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.ResourceUtil;
import com.google.gson.Gson;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class MsgConsumerFactory {
    static {
        ResourceUtil.addBundle("com.ai.paas.ipaas.mds.ipaas-message");
    }

    private MsgConsumerFactory() {
        // 禁止私有化
    }

    private static Map<String, IMessageConsumer> consumers = new ConcurrentHashMap<String, IMessageConsumer>();
    private static Map<String, IMessageConsumer> _consumers = new ConcurrentHashMap<String, IMessageConsumer>();

    public static IMessageConsumer getClient(AuthDescriptor ad, String topic,
                                             IMsgProcessorHandler msgProcessorHandler) {
        IMessageConsumer consumer = null;

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
        if (consumers.containsKey(ad.getServiceId() + "_" + topic)) {
            consumer = consumers.get(ad.getServiceId() + "_" + topic);
            return consumer;
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
            msgConf = CCSComponentFactory
                    .getConfigClient(authResult.getConfigAddr(),
                            authResult.getConfigUser(),
                            authResult.getConfigPasswd())
                    .get(MsgConstant.MSG_CONFIG_ROOT + ad.getServiceId()
                            + PaaSConstant.UNIX_SEPERATOR + topic + "/consumer");
        } catch (ConfigException e) {
            throw new MessageClientException(
                    "MsgConsumerFactory getClient error!", e);
        }
        // String msgConf = ConfigFactory.getConfigClient("127.0.0.1:2181",
        // "test", CiperUtil.encrypt("admin")).getConfig(
        // MsgConstant.MSG_CONFIG_ROOT + topic + "/consumer");
        // 封装成配置对象
        KafkaConfig kafkaConfig = null;
        Gson gson = new Gson();
        Properties props = gson.fromJson(msgConf, Properties.class);
        // 这里需要将topic加上
        props.put("kafka.topic", topic);
        props.put(Config.MDS_USER_SRV_ID, ad.getServiceId());
        try {
            // mds.partition.runninglock.path
            props.put(Config.MDS_PARTITION_RUNNING_LOCK_PATH, ConfigPathMode.appendPath(authResult.
                    getConfigUser(), ConfigPathMode.WRITABLE.getFlag(), MsgConstant.MSG_CONFIG_ROOT
                    + kafkaConfig._stateConf.get(Config.MDS_USER_SRV_ID)
                    + PaaSConstant.UNIX_SEPERATOR
                    + kafkaConfig._stateConf.get(Config.KAFKA_TOPIC)
                    + "/consumer/partitions/running"));


            //mds.partition.pauselock.path
            props.put(Config.MDS_PARTITION_PAUSE_LOCK_PATH, ConfigPathMode.appendPath(authResult.
                    getConfigUser(), ConfigPathMode.WRITABLE.getFlag(), MsgConstant.MSG_CONFIG_ROOT
                    + kafkaConfig._stateConf.get(Config.MDS_USER_SRV_ID)
                    + PaaSConstant.UNIX_SEPERATOR
                    + kafkaConfig._stateConf.get(Config.KAFKA_TOPIC)
                    + "/consumer/partitions/pause"));


            //mds.partition.offset.basepath
            props.put(Config.MDS_PARTITION_OFFSET_BASE_PATH, ConfigPathMode.appendPath(authResult.
                    getConfigUser(), ConfigPathMode.WRITABLE.getFlag(), MsgConstant.MSG_CONFIG_ROOT
                    + kafkaConfig._stateConf.get(Config.MDS_USER_SRV_ID)
                    + PaaSConstant.UNIX_SEPERATOR
                    + kafkaConfig._stateConf.get(Config.KAFKA_TOPIC)
                    + PaaSConstant.UNIX_SEPERATOR + "consumer"
                    + PaaSConstant.UNIX_SEPERATOR + "offsets"));


            //mds.consumer.base.path
            props.put(Config.MDS_CONSUMER_BASE_PATH, ConfigPathMode.appendPath(authResult.
                    getConfigUser(), ConfigPathMode.WRITABLE.getFlag(), MsgConstant.MSG_CONFIG_ROOT
                    + kafkaConfig._stateConf.get(Config.MDS_USER_SRV_ID)
                    + PaaSConstant.UNIX_SEPERATOR
                    + kafkaConfig._stateConf.get(Config.KAFKA_TOPIC)
                    + "/consumer/consumers"));
        } catch (Exception e) {
            throw new MessageClientException("MessageConsumer init error!", e);
        }


        kafkaConfig = new KafkaConfig(props);
        // 开始构建实例
        // AuthResult authResult=new AuthResult();
        // authResult.setConfigAddr("127.0.0.1:2181");
        // authResult.setConfigUser("test");
        // authResult.setConfigPasswd(CiperUtil.encrypt("test"));
        //consumer = new MessageConsumer(authResult, kafkaConfig,
        //        msgProcessorHandler);
        ZKClient zkClient = null;
        try {
            zkClient = ZKPoolFactory.getZKPool(authResult.getConfigAddr(), authResult.getConfigUser(),
                    authResult.getConfigPasswd(), 60000).getZkClient(authResult.getConfigAddr(),
                    authResult.getConfigUser());
        } catch (Exception e) {
            throw new MessageClientException("MessageConsumer init error!", e);
        }
        consumer = new MessageConsumer(zkClient, kafkaConfig,
                msgProcessorHandler);

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
            zkClient = ZKPoolFactory.getZKPool(authResult.getConfigAddr(), authResult.getConfigUser(), authResult.getConfigPasswd(), 60000)
                    .getZkClient(authResult.getConfigAddr(), authResult.getConfigUser());
        } catch (Exception e) {
            throw new MessageClientException("MessageConsumer init error!", e);
        }

        consumer = new MessageConsumer(zkClient, kafkaConfig,
                msgProcessorHandler);
        _consumers.put(topic, consumer);
        return consumer;
    }
}

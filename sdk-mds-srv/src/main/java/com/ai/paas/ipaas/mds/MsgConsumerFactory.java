package com.ai.paas.ipaas.mds;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.ai.paas.GeneralRuntimeException;
import com.ai.paas.ipaas.ccs.constants.ConfigException;
import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.mds.impl.consumer.MsgConsumer;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.util.Assert;
import com.ai.paas.util.ResourceUtil;

public class MsgConsumerFactory {
    static {
        ResourceUtil.addBundle("com.ai.paas.ipaas.mds.ipaas-message");
    }

    private MsgConsumerFactory() {
        // 禁止私有化
    }

    private static Map<String, IConsumer> consumers = new ConcurrentHashMap<>();
    private static Map<String, IConsumer> kafkaConsumers = new ConcurrentHashMap<>();

    /**
     * 支持多消费端，获取消息队列消费端
     * 
     * @param ad                  带服务标识身份认证信息
     * @param msgProcessorHandler 消息处理器
     * @param consumerId          消费者标识，默认为consumer，尽量不要使用consumer
     * @return
     */
    public static IConsumer getClient(AuthDescriptor ad, Processor processor, String consumerId) {
        IConsumer consumer = null;
        Assert.notNull(consumerId, "consumer id is null!");
        if ("consumer".equalsIgnoreCase(consumerId)) {
            throw new GeneralRuntimeException("consumer id ");
        }
        MsgUtil.validate(ad);
        AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
        MsgUtil.validateAuthResult(authResult);
        List<String> children = null;
        try {
            children = CCSComponentFactory.getConfigClient(authResult.getConfigAddr(), authResult.getConfigUser(),
                    authResult.getConfigPasswd()).listSubPath(MsgConstant.MSG_CONFIG_ROOT + ad.getServiceId());
        } catch (ConfigException e) {
            throw new MessageException("MsgSenderFactory getClient error!", e);
        }
        if (null == children || children.isEmpty()) {
            throw new MessageException("MsgSenderFactory can not get config info for:" + ad.getServiceId());
        }
        String topic = children.get(0);
        MsgUtil.validateTopic(topic);
        // 获取该用户申请的kafka服务配置信息
        consumer = MsgUtil.instanceConsumer(ad.getServiceId(), consumerId, authResult, topic, processor);
        return consumer;
    }

    /**
     * 获取消息队列消费端
     * 
     * @param ad                  含服务标识等信息的身份验证对象
     * @param msgProcessorHandler 如何处理消息的对象，里面的消息处理器和分区数对应
     * @return
     */
    public static IConsumer getClient(AuthDescriptor ad, Processor processor) {
        IConsumer consumer = null;

        MsgUtil.validate(ad);
        AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
        MsgUtil.validateAuthResult(authResult);
        List<String> children = null;
        try {
            children = CCSComponentFactory.getConfigClient(authResult.getConfigAddr(), authResult.getConfigUser(),
                    authResult.getConfigPasswd()).listSubPath(MsgConstant.MSG_CONFIG_ROOT + ad.getServiceId());
        } catch (ConfigException e) {
            throw new MessageException("MsgSenderFactory getClient error!", e);
        }
        if (null == children || children.isEmpty()) {
            throw new MessageException("MsgSenderFactory can not get config info for:" + ad.getServiceId());
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
        consumer = MsgUtil.instanceConsumer(ad.getServiceId(), authResult, topic,  processor);
        consumers.put(topic, consumer);
        return consumer;
    }

    /**
     * 获取消息队列消费端
     * 
     * @param ad                  含服务标识等信息的身份验证对象
     * @param topic               消息队列名称
     * @param msgProcessorHandler 如何处理消息的对象，里面的消息处理器和分区数对应
     * @return
     */
    public static IConsumer getClient(AuthDescriptor ad, String topic, Processor processor) {
        IConsumer consumer = null;

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
        consumer = MsgUtil.instanceConsumer(ad.getServiceId(), authResult, topic, processor);
        consumers.put(topic, consumer);
        return consumer;
    }

    /**
     * 获取消息队列消费端
     * 
     * @param authResult          身份验证结果，主要为了获取消息队列信息
     * @param props               队列信息 ，属性定义
     * @param topic               消息队列名称
     * @param msgProcessorHandler 如何处理消息的对象，里面的消息处理器和分区数对应
     * @return
     */
    public static IConsumer getClient(AuthResult authResult, Properties props, String topic, Processor processor) {
        IConsumer consumer = null;
        Assert.notNull(props, ResourceUtil.getMessage("com.ai.paas.ipaas.msg.cfg_null"));
        Assert.notNull(topic, ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
        if (kafkaConsumers.containsKey(topic))
            return kafkaConsumers.get(topic);
        // 这里需要将topic加上
        // 开始构建实例
        int partitions = 0;
        if (null != props.get(MsgConstant.PARTITION_NUM)) {
            partitions = Integer.parseInt((String) props.get(MsgConstant.PARTITION_NUM));
        }
        consumer = new MsgConsumer(props, topic, partitions, processor);
        kafkaConsumers.put(topic, consumer);
        return consumer;
    }
}

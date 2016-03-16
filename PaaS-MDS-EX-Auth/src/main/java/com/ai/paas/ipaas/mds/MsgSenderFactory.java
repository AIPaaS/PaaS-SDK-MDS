package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.impl.sender.MessageSender;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.ResourceUtil;
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


    public static IMessageSender getClient(Properties kafaProps, String userId,
                                           String topic) {
        IMessageSender sender = null;
        Assert.notNull(kafaProps,
                ResourceUtil.getMessage("com.ai.paas.ipaas.msg.cfg_null"));
        Assert.notNull(topic,
                ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
        if (_senders.containsKey(topic)) {
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

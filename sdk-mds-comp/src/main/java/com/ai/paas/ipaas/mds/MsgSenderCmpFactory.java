package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.impl.sender.MsgProducer;
import com.ai.paas.util.Assert;
import com.ai.paas.util.ResourceUtil;

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

    private static Map<String, IProducer> senders = new ConcurrentHashMap<>();

    public static IProducer getClient(Properties kafaProps, String topic) {
        IProducer sender = null;
        Assert.notNull(kafaProps, ResourceUtil.getMessage("com.ai.paas.ipaas.msg.cfg_null"));
        Assert.notNull(topic, ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
        if (null != senders.get(topic)) {
            return senders.get(topic);
        }
        // 开始构建实例
        int maxProducer = 0;
        if (null != kafaProps.get(MsgConstant.PROP_MAX_PRODUCER)) {
            maxProducer = Integer.parseInt((String) kafaProps.get(MsgConstant.PROP_MAX_PRODUCER));
        }
        sender = new MsgProducer(kafaProps, maxProducer, topic);
        senders.put(topic, sender);
        return sender;
    }
}

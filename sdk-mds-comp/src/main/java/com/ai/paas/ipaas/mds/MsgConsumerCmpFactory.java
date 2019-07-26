package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.impl.consumer.MsgConsumer;
import com.ai.paas.util.Assert;
import com.ai.paas.util.ResourceUtil;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class MsgConsumerCmpFactory {
    static {
        ResourceUtil.addBundle("com.ai.paas.ipaas.mds.ipaas-message");
    }

    private MsgConsumerCmpFactory() {
        // 禁止私有化
    }

    private static Map<String, IConsumer> consumers = new ConcurrentHashMap<>();

    /**
     * @param props
     * @param topic
     * @param msgProcessorHandler
     * @return
     */
    public static IConsumer getClient(Properties props, String topic, Processor processor) {
        IConsumer consumer = null;
        Assert.notNull(props, ResourceUtil.getMessage("com.ai.paas.ipaas.msg.cfg_null"));
        Assert.notNull(topic, ResourceUtil.getMessage("com.ai.paas.ipaas.msg.topic_null"));
        if (null != consumers.get(topic))
            return consumers.get(topic);
        int partitions = 1;
        if (null != props.get(MsgConstant.PARTITION_NUM)) {
            partitions = Integer.parseInt((String) props.get(MsgConstant.PARTITION_NUM));
        }
        consumer = new MsgConsumer(props, topic, partitions, processor);
        consumers.put(topic, consumer);
        return consumer;
    }
}

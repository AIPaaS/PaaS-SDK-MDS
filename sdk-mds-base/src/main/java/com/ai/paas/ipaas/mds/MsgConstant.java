package com.ai.paas.ipaas.mds;

import com.ai.paas.Constant;

public class MsgConstant extends Constant {
    /**
     * 消息的根路径
     */
    public static final String MSG_CONFIG_ROOT = "/MDS/";

    /**
     * 最大生产者
     */
    public static final String PROP_MAX_PRODUCER = "maxProducer";

    /**
     * 调整后的消费位置
     */
    public static final String CONSUMER_ADJUSTED_OFFSET = "adjusted_offset";

    public static final String PARTITION_NUM = "partition.num";
}

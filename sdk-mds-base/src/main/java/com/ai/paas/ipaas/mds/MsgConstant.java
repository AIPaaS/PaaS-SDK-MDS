package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.PaaSConstant;

public class MsgConstant extends PaaSConstant {
	/**
	 * 消息的根路径
	 */
	public final static String MSG_CONFIG_ROOT = "/MDS/";

	/**
	 * 最大生产者
	 */
	public final static String PROP_MAX_PRODUCER = "maxProducer";

	/**
	 * 调整后的消费位置
	 */
	public final static String CONSUMER_ADJUSTED_OFFSET = "adjusted_offset";

	public final static String PARTITION_NUM = "partition.num";
}

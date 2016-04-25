
package com.ai.paas.ipaas.mds.impl.consumer.client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.HashMap;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtils {
	private static transient final Logger logger = LoggerFactory
			.getLogger(KafkaUtils.class);

	private static final int NO_OFFSET = -5;

	public static long getOffset(SimpleConsumer consumer, String topic,
			int partition, KafkaConfig config) {
		long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		if (config._forceFromStart) {
			startOffsetTime = config._startOffsetTime;
		}
		return getOffset(consumer, topic, partition, startOffsetTime);
	}

	public static long getOffset(SimpleConsumer consumer, String topic,
			int partition, long startOffsetTime) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				startOffsetTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

		long[] offsets = consumer.getOffsetsBefore(request).offsets(topic,
				partition);
		if (offsets.length > 0) {
			return offsets[0];
		} else {
			return NO_OFFSET;
		}
	}

	public static FetchResponse fetchMessages(KafkaConfig config,
			SimpleConsumer consumer, Partition partition, long offset) {
		@SuppressWarnings("unused")
		ByteBufferMessageSet msgs = null;
		String topic = (String) config._stateConf.get(Config.KAFKA_TOPIC);
		int partitionId = partition.partition;

		// for (int errors = 0; errors < 2 && msgs == null; errors++) {
		FetchRequestBuilder builder = new FetchRequestBuilder();
		FetchRequest fetchRequest = builder
				.addFetch(topic, partitionId, offset, config._fetchSizeBytes)
				.clientId(
						(String) config._stateConf
								.get(Config.KAFKA_CONSUMER_ID)).build();
		FetchResponse fetchResponse;
		try {
			fetchResponse = consumer.fetch(fetchRequest);
		} catch (Exception e) {
			if (e instanceof ConnectException
					|| e instanceof SocketTimeoutException
					|| e instanceof IOException
					|| e instanceof UnresolvedAddressException) {

				logger.warn("Network error when fetching messages:", e);
				throw new FailedFetchException(e);
			} else {
				throw new RuntimeException(e);
			}
		}

		return fetchResponse;
	}
}

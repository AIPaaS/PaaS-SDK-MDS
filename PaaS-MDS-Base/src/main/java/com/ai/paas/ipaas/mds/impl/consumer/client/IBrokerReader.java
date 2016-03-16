
/*
 *   This file is based on the source code of the Kafka spout of the Apache Storm project.
 *   (https://github.com/apache/storm/tree/master/external/storm-kafka)
 */

package com.ai.paas.ipaas.mds.impl.consumer.client;

public interface IBrokerReader {

	GlobalPartitionInformation getCurrentBrokers();

	void close();
}

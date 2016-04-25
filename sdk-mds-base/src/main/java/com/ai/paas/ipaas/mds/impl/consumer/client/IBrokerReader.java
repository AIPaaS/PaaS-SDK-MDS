

package com.ai.paas.ipaas.mds.impl.consumer.client;

public interface IBrokerReader {

	GlobalPartitionInformation getCurrentBrokers();

	void close();
}

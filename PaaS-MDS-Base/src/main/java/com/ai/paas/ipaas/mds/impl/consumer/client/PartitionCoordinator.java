/*
 *   This file is based on the source code of the Kafka spout of the Apache Storm project.
 *   (https://github.com/apache/storm/tree/master/external/storm-kafka)
 */

package com.ai.paas.ipaas.mds.impl.consumer.client;

import java.io.Serializable;
import java.util.List;

public interface PartitionCoordinator extends Serializable {
	List<PartitionManager> getMyManagedPartitions();

	PartitionManager getManager(Partition partition);

	void refresh();
}

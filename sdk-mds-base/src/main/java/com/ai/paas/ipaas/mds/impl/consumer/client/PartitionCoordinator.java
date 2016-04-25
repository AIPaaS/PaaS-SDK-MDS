
package com.ai.paas.ipaas.mds.impl.consumer.client;

import java.io.Serializable;
import java.util.List;

public interface PartitionCoordinator extends Serializable {
	List<PartitionManager> getMyManagedPartitions();

	PartitionManager getManager(Partition partition);

	void refresh();
}

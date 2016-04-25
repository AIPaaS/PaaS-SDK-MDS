package com.ai.paas.ipaas.mds.impl.sender;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class ModPartitioner implements Partitioner {

	public ModPartitioner(VerifiableProperties props) {
		
	}
	
	public int partition(Object id, int partitionNumber) {
		long key = Math.abs(Long.parseLong(id.toString()));
		return (int)(key%partitionNumber);
	}
	
	
}

package com.ai.paas.ipaas.mds.vo;

import java.io.Serializable;

public class MessageAndMetadata implements Serializable {

	private static final long serialVersionUID = 9152243091714512036L;
	private byte[] message;
	private byte[] key;
	private long offset;
	private String partitionId;
	private String topic;

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public byte[] getMessage() {
		return message;
	}

	public void setMessage(byte[] message) {
		this.message = message;
	}

	public String getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(String partitionId) {
		this.partitionId = partitionId;
	}

}

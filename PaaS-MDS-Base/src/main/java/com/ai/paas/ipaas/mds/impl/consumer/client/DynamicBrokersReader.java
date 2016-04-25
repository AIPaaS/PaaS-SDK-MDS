
package com.ai.paas.ipaas.mds.impl.consumer.client;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class DynamicBrokersReader implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2430520158927968400L;

	private static transient final Logger logger = LoggerFactory.getLogger(DynamicBrokersReader.class);

	transient private ZKClient _zkClient;
	private String _zkPath;
	private String _topic;

	public DynamicBrokersReader(KafkaConfig config, ZkState zkState) {
		_zkPath = (String) config._stateConf
				.get(Config.KAFKA_ZOOKEEPER_BROKER_PATH);
		_topic = (String) config._stateConf.get(Config.KAFKA_TOPIC);
		_zkClient = zkState.getZKClient();
	}

	/**
	 * Get all partitions with their current leaders
	 */
	public GlobalPartitionInformation getBrokerInfo() {
		GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
		try {
			int numPartitionsForTopic = getNumPartitions();
			String brokerInfoPath = brokerPath();
			for (int partition = 0; partition < numPartitionsForTopic; partition++) {
				int leader = getLeaderFor(partition);
				String path = brokerInfoPath + "/" + leader;
				byte[] brokerData = _zkClient.getNodeBytes(path, null);
				Broker hp = getBrokerHost(brokerData);
				globalPartitionInformation.addPartition(partition, hp);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		logger.info("Read partition info from zookeeper: "
				+ globalPartitionInformation);
		return globalPartitionInformation;
	}

	public int getNumPartitions() {
		try {
			String topicBrokersPath = partitionPath();
			List<String> children = _zkClient.getChildren(topicBrokersPath);
			return children.size();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String partitionPath() {
		return _zkPath + "/topics/" + _topic + "/partitions";
	}

	public String brokerPath() {
		return _zkPath + "/ids";
	}

	/**
	 * get /brokers/topics/distributedTopic/partitions/1/state {
	 * "controller_epoch":4, "isr":[ 1, 0 ], "leader":1, "leader_epoch":1,
	 * "version":1 }
	 * 
	 * @param partition
	 * @return
	 */
	private int getLeaderFor(long partition) {
		try {
			String topicBrokersPath = partitionPath();
			byte[] hostPortData = _zkClient.getNodeBytes(topicBrokersPath + "/"
					+ partition + "/state", null);
			JsonObject root = new Gson().fromJson(new String(hostPortData,
					"UTF-8"), JsonObject.class);
			Integer leader = root.get("leader").getAsInt();
			return leader;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		_zkClient.quit();
	}

	/**
	 * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0 {
	 * "host":"localhost", "jmx_port":9999, "port":9092, "version":1 }
	 * 
	 * @param contents
	 * @return
	 */
	private Broker getBrokerHost(byte[] contents) {
		try {
			JsonObject root = new Gson().fromJson(
					new String(contents, "UTF-8"), JsonObject.class);
			String host = root.get("host").getAsString();
			Integer port = root.get("port").getAsInt();
			return new Broker(host, port);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

}

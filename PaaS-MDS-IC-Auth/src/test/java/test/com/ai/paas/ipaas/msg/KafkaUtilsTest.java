package test.com.ai.paas.ipaas.msg;

import java.util.Properties;

import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.mds.impl.consumer.client.Config;
import com.ai.paas.ipaas.mds.impl.consumer.client.DynamicBrokersReader;
import com.ai.paas.ipaas.mds.impl.consumer.client.DynamicPartitionConnections;
import com.ai.paas.ipaas.mds.impl.consumer.client.GlobalPartitionInformation;
import com.ai.paas.ipaas.mds.impl.consumer.client.KafkaConfig;
import com.ai.paas.ipaas.mds.impl.consumer.client.KafkaUtils;
import com.ai.paas.ipaas.mds.impl.consumer.client.Partition;
import com.ai.paas.ipaas.mds.impl.consumer.client.ZkBrokerReader;
import com.ai.paas.ipaas.mds.impl.consumer.client.ZkState;
import com.google.gson.Gson;

public class KafkaUtilsTest {
	@Before
	public void setUp() throws Exception {

	}

	@Test
	public void fetchMessages() throws Exception {
		int partitionId = 0;
		String topic = "signatureId-89e62a17-2560-4ab5-a2b1-f09dc5c351a5";
		KafkaConfig config = null;
		Gson gson = new Gson();
		String msgConf = "{'kafka.zookeeper.hosts':'10.1.228.198:39181,10.1.228.199:39181,10.1.228.200:39181','kafka.zookeeper.broker.path':'/brokers','kafka.consumer.id':'12345'}";
		Properties props = gson.fromJson(msgConf, Properties.class);
		// 这里需要将topic加上
		props.put("kafka.topic", topic);
		props.put(Config.MDS_USER_SRV_ID, "D60E7D1549384A6AB0C4419EA33920CE");
		config = new KafkaConfig(props);
		ZkState state = new ZkState(config);
		DynamicPartitionConnections connections = new DynamicPartitionConnections(
				config, new ZkBrokerReader(config, state));
		DynamicBrokersReader reader = new DynamicBrokersReader(config, state);
		GlobalPartitionInformation brokerInfo = reader.getBrokerInfo();

		Partition partition = new Partition(
				brokerInfo.getBrokerFor(partitionId), partitionId);
		SimpleConsumer consumer = connections.register(partition.host,
				partition.partition);

		long offset = 248;

		FetchResponse response = KafkaUtils.fetchMessages(config, consumer,
				partition, offset);
		ByteBufferMessageSet msgs = response.messageSet(topic,
				partition.partition);
		for (MessageAndOffset msgAndOffset : msgs) {
			if (msgAndOffset.message() != null) {
				Message msg = msgAndOffset.message();
				byte[] payload = new byte[msg.payload().remaining()];
				msg.payload().get(payload);
				System.out.println(new String(payload, "UTF-8"));
			} else {
				System.out.println("------------");
			}
		}

	}
}

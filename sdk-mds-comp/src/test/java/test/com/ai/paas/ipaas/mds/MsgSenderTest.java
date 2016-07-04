package test.com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.IMessageSender;
import com.ai.paas.ipaas.mds.MsgSenderCmpFactory;
import org.junit.Test;

import java.util.Properties;

public class MsgSenderTest {

    @Test
    public void testSenderMessage() {
        Properties properties = new Properties();
        properties.setProperty("metadata.broker.list","127.0.0.1:9092");
        properties.setProperty("serializer.class","kafka.serializer.DefaultEncoder");
        properties.setProperty("key.serializer.class","kafka.serializer.StringEncoder");
        properties.setProperty("partitioner.class","com.ai.paas.ipaas.mds.impl.sender.ModPartitioner");
        properties.setProperty("request.required.acks","1");
        properties.setProperty("queue.buffering.max.messages","1048576");
        properties.setProperty("producer.type","sync");
        properties.setProperty("message.send.max.retries","3");
        properties.setProperty("compression.codec","none");
        properties.setProperty("request.timeout.ms","20000");
        properties.setProperty("batch.num.messages","64000");
        properties.setProperty("send.buffer.bytes","67108864");
        properties.setProperty("maxProducer","5");

        String topicId = "MDS-TEST";
        IMessageSender sender = MsgSenderCmpFactory.getClient(properties, topicId);
        sender.send("Hello World", 1);
    }
}

package test.com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.IConsumer;
import com.ai.paas.ipaas.mds.MsgConsumerCmpFactory;
import com.ai.paas.ipaas.mds.Processor;
import com.ai.paas.util.UUIDTool;

import java.util.Properties;

import org.junit.Test;

public class MsgConsumerTest {
	
	@Test
    public void testConsumerMessage()  {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.12.2.144:19092,10.12.2.145:19092,10.12.2.146:19092");
        properties.setProperty("group.id", UUIDTool.genShortId());
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        

        String topic = "DOUBO_TEST";
        Processor processor=new MessageProcessor();
        IConsumer sender = MsgConsumerCmpFactory.getClient(properties, topic, processor);
        sender.start();
    }
}

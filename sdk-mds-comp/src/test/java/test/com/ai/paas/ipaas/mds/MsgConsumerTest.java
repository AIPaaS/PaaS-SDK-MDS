package test.com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.IMessageConsumer;
import com.ai.paas.ipaas.mds.IMsgProcessorHandler;
import com.ai.paas.ipaas.mds.MsgConsumerCmpFactory;

import java.util.Properties;

import org.junit.Test;

public class MsgConsumerTest {
	
	@Test
    public void testConsumerMessage() throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("kafka.zookeeper.hosts", "10.1.245.224:30181,10.1.245.225:30281,10.1.245.8:30281");
        properties.setProperty("kafka.zookeeper.broker.path", "/brokers");
        properties.setProperty("kafka.zookeeper.user", "");
        properties.setProperty("kafka.zookeeper.user.passwd", "");
        properties.setProperty("kafka.consumer.id", "123456");
        properties.setProperty("mds.partition.runninglock.path", "/baas/MDS/zhangxin10/MDS-TEST/consumer/partitions");
        properties.setProperty("mds.partition.pauselock.path", "/baas/MDS/zhangxin10/MDS-TEST/consumer/partitions");
        properties.setProperty("mds.partition.offset.basepath", "/baas/MDS/zhangxin10/MDS-TEST/consumer/" +
                "offsets");
        properties.setProperty("mds.consumer.base.path", "/baas/MDS/zhangxin10/MDS-TEST");
        properties.setProperty("mds.zookeeper.hosts", "10.1.245.224:30181,10.1.245.225:30281,10.1.245.8:30281");

        String topicId = "test123";
        IMsgProcessorHandler processorClass = new ProcessorClass();
        IMessageConsumer sender = MsgConsumerCmpFactory.getClient(properties, topicId, processorClass);
        sender.start();
        while (true) {
            Thread.sleep(1000L);
        }
    }
}

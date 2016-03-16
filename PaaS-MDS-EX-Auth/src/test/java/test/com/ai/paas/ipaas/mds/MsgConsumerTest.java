package test.com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.*;
import org.junit.Test;

import java.util.Properties;

public class MsgConsumerTest {

    @Test
    public void testConsumerMessage() throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("kafka.zookeeper.hosts", "127.0.0.1:2181");
        properties.setProperty("kafka.zookeeper.broker.path","/brokers");
        properties.setProperty("kafka.zookeeper.user", "");
        properties.setProperty("kafka.zookeeper.user.passwd","");
        properties.setProperty("kafka.consumer.id", "123456");
        properties.setProperty("mds.partition.runninglock.path","/baas/MDS/zhangxin10/MDS-TEST/consumer/partitions");
        properties.setProperty("mds.partition.pauselock.path","/baas/MDS/zhangxin10/MDS-TEST/consumer/partitions");
        properties.setProperty("mds.partition.offset.basepath","/baas/MDS/zhangxin10/MDS-TEST/consumer/" +
                "offsets");
        properties.setProperty("mds.consumer.base.path","/baas/MDS/zhangxin10/MDS-TEST");
        properties.setProperty("mds.zookeeper.hosts", "127.0.0.1:2181");

        String topicId = "MDS-TEST";
        IMsgProcessorHandler processorClass = new ProcessorClass();
        IMessageConsumer sender = MsgConsumerFactory.getClient(properties, topicId, processorClass);
        sender.start();
        while(true){
            Thread.sleep(1000L);
        }
    }
}

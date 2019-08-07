package test.com.ai.paas.ipaas.mds;

import java.util.LinkedHashMap;
import java.util.Properties;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.mds.IProducer;
import com.ai.paas.ipaas.mds.MsgSenderCmpFactory;

public class MsgSenderTest {
    private static final Logger log = LoggerFactory.getLogger(MsgSenderTest.class);

    @Test
    public void testSenderMessage() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.12.2.144:19092,10.12.2.145:19092,10.12.2.146:19092");
        properties.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty("compression.codec", "");
        properties.setProperty("request.timeout.ms", "120000");
        properties.setProperty("maxProducer", "1");

        String topic = "DOUBO_TEST";
        IProducer sender = MsgSenderCmpFactory.getClient(properties, topic);
        LinkedHashMap<String, String> msgs = new LinkedHashMap<>();
        for (int i = 0; i < 1000; i++) {
            msgs.put("i" + i, "This ia test message!" + i);
        }
        sender.send(msgs);
        sender.send(0, msgs);
        sender.send(1, msgs);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            sender.asyncSend("bbbb" + i, "Hello World");
            log.info("---{}-", i);
        }
        sender.flush();
        log.info("---{}-", (System.currentTimeMillis() - start));
    }
}

package com.ai.paas.ipaas.mds;

import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.mds.impl.sender.MsgProducer;

public class SenderTest {
    private static IProducer sender = null;
    private static final Logger log = LoggerFactory.getLogger(SenderTest.class);
    private static String topic = "DOUBO_TEST";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.12.2.144:19092,10.12.2.145:19092,10.12.2.146:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        sender = new MsgProducer(props, 10, topic);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        sender.close();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSend() {
        sender.send("18600127780", "This ia test message!");
        log.info(StringSerializer.class.getName());
    }

    @Test
    public void testParitions() {
        assertTrue(sender.getParititions() == 2);
    }

    @Test
    public void testParitionSend() {
        sender.send(0, "18600127781", "This ia test message!18600127781");
        sender.send(1, "18600127782", "This ia test message!18600127782");
    }

    @Test
    public void testBatchSend() {
        LinkedHashMap<String, String> msgs = new LinkedHashMap<>();
        for (int i = 0; i < 1000; i++) {
            msgs.put("i" + i, "This ia test message!" + i);
        }
        sender.send(msgs);
        sender.send(0, msgs);
        sender.send(1, msgs);
    }

    @Test
    public void testAsyncSend() throws Exception {

        sender.send("18511062128", "This is a test!", new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    log.info("----{}", metadata.offset());
                }
            }
        });

    }
}

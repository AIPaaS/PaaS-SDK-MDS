package com.ai.paas.ipaas.mds;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.mds.impl.consumer.MsgConsumer;

public class ConsumerTest {

    private static final Logger log = LoggerFactory.getLogger(ConsumerTest.class);
    private static IConsumer consumer = null;
    private static String topic = "DOUBO_TEST";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.12.2.144:19092,10.12.2.145:19092,10.12.2.146:19092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Processor processor = new MsgProcessor();
        consumer = new MsgConsumer(props, topic, 2, processor);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testConsumerMsg() {
        consumer.start();
    }

}

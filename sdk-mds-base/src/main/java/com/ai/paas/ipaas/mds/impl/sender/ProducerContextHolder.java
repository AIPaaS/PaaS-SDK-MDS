package com.ai.paas.ipaas.mds.impl.sender;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.LoggerFactory;

public class ProducerContextHolder {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ProducerContextHolder.class);
    private static final ThreadLocal<Producer<String, byte[]>> producerHolder = new ThreadLocal<>();

    private ProducerContextHolder() {

    }

    public static void clean() {
        log.info("Start to clean producer....");
        producerHolder.set(null);
    }

    public static void setProducer(Producer<String, byte[]> producer) {
        producerHolder.set(producer);
    }

    public static Producer<String, byte[]> getProducer() {
        return producerHolder.get();
    }
}

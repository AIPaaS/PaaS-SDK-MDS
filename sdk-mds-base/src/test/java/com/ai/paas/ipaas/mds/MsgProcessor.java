package com.ai.paas.ipaas.mds;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsgProcessor implements Processor {
    private static final Logger log = LoggerFactory.getLogger(MsgProcessor.class);

    @Override
    public void process(ConsumerRecord<String, String> message) throws Exception {
        log.info("-------------------------------{}---------", message.value());

    }

    @Override
    public void process(ConsumerRecords<String, String> consumerRecords) throws Exception {

        consumerRecords.forEach(record ->
            {
                log.info("-----------------------{}--------{}---------====", record.key(), record.value());
            });
    }

    @Override
    public boolean isBatch() {
        return true;
    }

    @Override
    public int getBatchSize() {
        return 0;
    }

}


package com.ai.paas.ipaas.mds.impl.consumer;

import com.ai.paas.ipaas.mds.Processor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@SuppressWarnings("rawtypes")
public class ConsumerThread implements Callable, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
    private static final long serialVersionUID = 1780042755212645597L;
    private transient Consumer<String, String> consumer = null;
    private transient Processor processor = null;
    private transient boolean stoped = false;

    public ConsumerThread(Consumer<String, String> consumer, Processor processor) {
        this.consumer = consumer;
        this.processor = processor;
        stoped = false;
    }

    public void run() {
        final int giveUp = 100;
        int noRecordsCount = 0;
        logger.info("start to consumer kafka messages....topic:{}", consumer.listTopics());
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            while (!stoped) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

                // 这里应该是休息一段时间
                if (consumerRecords.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > giveUp) {
                        try {
                            Thread.sleep(10000);
                        } catch (Exception e) {
                            logger.error("sleerp error!", e);
                        }
                        noRecordsCount = 0;
                    } else
                        continue;
                }
                if (processor.isBatch()) {
                    try {
                        processor.process(consumerRecords);
                    } catch (Exception e) {
                        // 如果抛出异常，而且是批量抛出，怎么提交offset，全部回退，则重复消费
                        // 需要外面的事务是一致的
                        // 什么也不做
                        throw new RuntimeException("consumer record:{}" + consumerRecords, e);
                    }
                } else {
                    consumerRecords.forEach(record ->
                        {
                            try {
                                processor.process(record);
                                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset());
                                offsets.put(partition, offsetAndMetadata);
                            } catch (Exception e) {
                                // 有异常发生,提交以前的
                                consumer.commitSync(offsets);
                                // 然后抛出异常，停止运行，外面还得停止
                                throw new RuntimeException("consumer record:{}" + record, e);
                            }
                        });
                }
                // 同步提交，此时有可能出现消息消费了，提交出错了，用户可以传进来自己的offset
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }

    }

    public void stop() {
        stoped = true;
    }

    @Override
    public Object call() throws Exception {
        run();
        return null;
    }

}

package com.ai.paas.ipaas.mds.impl.consumer;

import com.ai.paas.ipaas.mds.*;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MsgConsumer implements IConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MsgConsumer.class);

    // 创建一个线程池,数量要尽量大于分区数，要考虑到如何启动以及如何和springboot集成启动
    private Processor processor;
    private Properties props;
    private String topic = null;
    private int partitions;
    private ExecutorService service = null;
    private List<ConsumerThread> consumers = new ArrayList<>();

    public MsgConsumer(Properties props, String topic, int partitions, Processor processor) {
        this.props = props;
        this.topic = topic;
        this.processor = processor;
        this.partitions = partitions;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void init() {
        Consumer<String, String> consumer = null;
        ConsumerThread thread = null;
        // 声明一个线程池，开始跑
        List<Future> futures = new ArrayList<>();
        Future future = null;
        service = Executors.newFixedThreadPool(partitions);
        for (int i = 0; i < partitions; i++) {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));
            thread = new ConsumerThread(consumer, processor);
            consumers.add(thread);
            future = service.submit(thread);
            futures.add(future);
        }

        for (Future f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                // 停止整个线程池
                logger.error("", e);
                stop();
                throw new RuntimeException(e);
            }
        }
    }

    public void start() {
        // 初始化并开始运行
        init();
    }

    public void stop() {
        // 停止所有的线程？
        logger.info("Start to stopping all message consumers....");
        for (ConsumerThread consumer : consumers) {
            try {
                consumer.stop();
            } catch (Exception e) {
                // 停止整个线程池
                logger.error("error to stop thread!", e);
            }
        }
        try {
            service.awaitTermination(10000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("stop kafka consumer error!", e);
        }
        service.shutdown();
    }
}

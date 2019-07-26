package com.ai.paas.ipaas.mds.impl.sender;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.mds.IProducer;
import com.ai.paas.ipaas.mds.MessageException;
import com.ai.paas.util.Assert;
import com.ai.paas.util.UUIDTool;

public class MsgProducer implements IProducer {
    private static final Logger logger = LoggerFactory.getLogger(MsgProducer.class);
    private Properties props = null;
    private String topic = null;
    private List<Producer<String, byte[]>> generalPool = new ArrayList<>();
    private List<Producer<String, byte[]>> transPool = new ArrayList<>();
    private int maxProducer = 0;
    private Properties transProps = new Properties();

    public MsgProducer(Properties props, int maxProducer, String topic) {
        this.props = props;
        this.topic = topic;
        this.maxProducer = maxProducer;
        transProps.putAll(props);
        transProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUIDTool.genShortId());
        // 初始化
        initProducerPool();
    }

    private void initProducerPool() {
        if (maxProducer <= 0) {
            maxProducer = 1;
        }
        Producer<String, byte[]> producer = null;
        for (int i = 0; i < maxProducer; i++) {
            producer = new KafkaProducer<>(props);
            generalPool.add(producer);
            producer = new KafkaProducer<>(transProps);
            transPool.add(producer);
        }
    }

    private Producer<String, byte[]> getInstance() {
        int size = generalPool.size();
        Producer<String, byte[]> producer = generalPool.get(new Random().nextInt(size));
        if (null == producer)
            producer = new KafkaProducer<>(props);
        return producer;
    }

    private Producer<String, byte[]> getTransInstance() {
        int size = transPool.size();
        Producer<String, byte[]> producer = transPool.get(new Random().nextInt(size));
        if (null == producer)
            producer = new KafkaProducer<>(transProps);
        return producer;
    }

    private void destoryProducer(Producer<String, byte[]> producer) {
        if (null != producer) {
            try {
                producer.close();
            } catch (Exception e) {
                logger.error("", e);
            }
        }
        generalPool.remove(producer);
        producer = new KafkaProducer<>(props);
        generalPool.add(producer);
    }

    private void destoryTransProducer(Producer<String, byte[]> producer) {
        if (null != producer) {
            try {
                producer.close();
            } catch (Exception e) {
                logger.error("", e);
            }
        }
        transPool.remove(producer);
        producer = new KafkaProducer<>(transProps);
        transPool.add(producer);
    }

    private void returnResource(Producer<String, byte[]> producer) {
        // do nothing
    }

    @Override
    public void send(String key, String msg) {
        // 从连接池获取一个实例，进行发送
        Assert.notNull(msg, "The message cnt is null!");
        Producer<String, byte[]> producer = null;
        try {
            producer = getInstance();
            producer.send(new ProducerRecord<String, byte[]>(topic, key, msg.getBytes(StandardCharsets.UTF_8))).get();
        } catch (Exception e) {
            destoryProducer(producer);
            throw new MessageException("Message send error:", e);
        } finally {
            if (null != producer) {
                returnResource(producer);
            }
        }
    }

    @Override
    public void send(int partition, String key, String msg) {
        // 从连接池获取一个实例，进行发送
        Assert.notNull(msg, "The message is null!");
        Producer<String, byte[]> producer = null;
        try {
            producer = getInstance();
            producer.send(
                    new ProducerRecord<String, byte[]>(topic, partition, key, msg.getBytes(StandardCharsets.UTF_8)))
                    .get();
        } catch (Exception e) {
            destoryProducer(producer);
            throw new MessageException("Error send msg:", e);
        } finally {
            if (null != producer) {
                returnResource(producer);
            }
        }
    }

    @Override
    public void send(String key, byte[] msg) {
        // 从连接池获取一个实例，进行发送
        Assert.notNull(msg, "The message is null!");
        Assert.notNull(key, "The message key is null!");
        Producer<String, byte[]> producer = null;
        try {
            producer = getInstance();
            producer.send(new ProducerRecord<String, byte[]>(topic, key, msg)).get();
        } catch (Exception e) {
            destoryProducer(producer);
            throw new MessageException("Message send error:", e);
        } finally {
            if (null != producer) {
                returnResource(producer);
            }
        }
    }

    @Override
    public void send(int partition, String key, byte[] msg) {
        // 从连接池获取一个实例，进行发送
        Assert.notNull(msg, "The message is null!");
        Assert.notNull(key, "The message key is null!");
        Producer<String, byte[]> producer = null;
        try {
            producer = getInstance();
            producer.send(new ProducerRecord<String, byte[]>(topic, partition, key, msg));
        } catch (Exception e) {
            destoryProducer(producer);
            throw new MessageException("MessageSender send:", e);
        } finally {
            if (null != producer) {
                returnResource(producer);
            }
        }
    }

    @Override
    public int getParititions() {
        Producer<String, byte[]> producer = getInstance();
        List<PartitionInfo> pInfos = producer.partitionsFor(topic);
        return null != pInfos ? pInfos.size() : 0;
    }

    @Override
    public void send(String... msgs) {
        Assert.notNull(msgs, "The messages is null!");
        Producer<String, byte[]> producer = null;
        producer = getTransInstance();
        producer.initTransactions();
        producer.beginTransaction();
        try {
            for (String msg : msgs) {
                producer.send(new ProducerRecord<String, byte[]>(topic, null, msg.getBytes(StandardCharsets.UTF_8)));
            }
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            destoryTransProducer(producer);
            throw new MessageException("MessageSender send:", e);
        } finally {
            returnResource(producer);
        }

    }

    public void send(LinkedHashMap<String, String> msgs) {
        Assert.notNull(msgs, "The messages is null!");
        Producer<String, byte[]> producer = null;
        producer = getTransInstance();
        producer.initTransactions();
        producer.beginTransaction();
        try {
            for (Map.Entry<String, String> msg : msgs.entrySet()) {
                producer.send(new ProducerRecord<String, byte[]>(topic, msg.getKey(),
                        msg.getValue().getBytes(StandardCharsets.UTF_8)));
            }
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            destoryTransProducer(producer);
            throw new MessageException("MessageSender send:", e);
        } finally {
            returnResource(producer);
        }
    }

    @Override
    public void send(int partition, LinkedHashMap<String, String> msgs) {
        Assert.notNull(msgs, "The messages is null!");
        Producer<String, byte[]> producer = null;
        producer = getTransInstance();
        producer.initTransactions();
        producer.beginTransaction();
        try {
            for (Map.Entry<String, String> msg : msgs.entrySet()) {
                producer.send(new ProducerRecord<String, byte[]>(topic, partition, msg.getKey(),
                        msg.getValue().getBytes(StandardCharsets.UTF_8)));
            }
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            destoryTransProducer(producer);
            throw new MessageException("MessageSender send:", e);
        } finally {
            returnResource(producer);
        }
    }

    @Override
    public void send(String key, String msg, Callback callback) {
        Assert.notNull(msg, "The messages is null!");
        Producer<String, byte[]> producer = null;
        producer = getInstance();
        try {

            producer.send(new ProducerRecord<String, byte[]>(topic, key, msg.getBytes(StandardCharsets.UTF_8)),
                    callback);
        } catch (Exception e) {
            destoryProducer(producer);
            throw new MessageException("MessageSender send:", e);
        } finally {
            returnResource(producer);
        }
    }

    @Override
    public void send(int partition, String key, String msg, Callback callback) {
        Assert.notNull(msg, "The messages is null!");
        Producer<String, byte[]> producer = null;
        producer = getInstance();
        try {
            producer.send(
                    new ProducerRecord<String, byte[]>(topic, partition, key, msg.getBytes(StandardCharsets.UTF_8)),
                    callback);
        } catch (Exception e) {
            destoryProducer(producer);
            throw new MessageException("MessageSender send:", e);
        } finally {
            returnResource(producer);
        }
    }

    @Override
    public Producer<String, byte[]> getProducer() {
        return getInstance();
    }

    @Override
    public Producer<String, byte[]> getTransProducer() {
        return getTransInstance();
    }

    @Override
    public void close() {
        for (Producer<String, byte[]> producer : generalPool) {
            producer.close();
        }
        for (Producer<String, byte[]> producer : transPool) {
            producer.close();
        }
    }

    @Override
    public void asyncSend(String key, String msg) {
        // 从连接池获取一个实例，进行发送
        Assert.notNull(msg, "The message cnt is null!");
        Producer<String, byte[]> producer = null;
        try {
            producer = getInstance();
            ProducerContextHolder.setProducer(producer);
            producer.send(new ProducerRecord<String, byte[]>(topic, key, msg.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            destoryProducer(producer);
            throw new MessageException("Message send error:", e);
        } finally {
            if (null != producer) {
                returnResource(producer);
            }
        }

    }

    @Override
    public void asyncSend(int partition, String key, String msg) {
        // 从连接池获取一个实例，进行发送
        Assert.notNull(msg, "The message is null!");
        Producer<String, byte[]> producer = null;
        try {
            producer = getInstance();
            ProducerContextHolder.setProducer(producer);
            producer.send(
                    new ProducerRecord<String, byte[]>(topic, partition, key, msg.getBytes(StandardCharsets.UTF_8)))
                    .get();
        } catch (Exception e) {
            destoryProducer(producer);
            throw new MessageException("Error send msg:", e);
        } finally {
            if (null != producer) {
                returnResource(producer);
            }
        }

    }

    @Override
    public void flush() {
        Producer<String, byte[]> producer = null;
        producer = ProducerContextHolder.getProducer();
        if (null != producer)
            producer.flush();
        ProducerContextHolder.clean();
    }

}

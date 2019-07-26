package com.ai.paas.ipaas.mds;

import java.util.LinkedHashMap;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;

public interface IProducer {

    /**
     * 发送一条字符串消息
     * 
     * @param key 消息键
     * @param msg 消息体
     * 
     */
    public void send(String key, String msg);

    /**
     * 异步发送，性能较高
     * 
     * @param key
     * @param msg
     */
    public void asyncSend(String key, String msg);

    /**
     * 发送一条字符串消息
     * 
     * @param key       消息键
     * @param msg       消息体
     * @param partition 具体分区，从0开始
     */
    public void send(int partition, String key, String msg);

    /**
     * 异步发送，性能较高
     * 
     * @param partition
     * @param key
     * @param msg
     */
    public void asyncSend(int partition, String key, String msg);

    /**
     * 发送一条字符串消息
     * 
     * @param key 消息键
     * @param msg 消息体
     * 
     */
    public void send(String key, byte[] msg);

    /**
     * 发送一条字符串消息
     * 
     * @param key       消息键
     * @param msg       消息体
     * @param partition 具体分区，从0开始
     */
    public void send(int partition, String key, byte[] msg);

    /**
     * 发送批量消息
     * 
     * @param key
     * @param msg
     */
    public void send(String... msgs);

    /**
     * 
     * @param msgs
     */
    public void send(LinkedHashMap<String, String> msgs);

    /**
     * 发送批量消息
     * 
     * @param partition
     * @param key
     * @param msgs
     */
    public void send(int partition, LinkedHashMap<String, String> msgs);

    /**
     * 获取分区数量
     * 
     * @return
     */
    public int getParititions();

    /**
     * 异步发送，完成或者异常是给出结果
     * 
     * @param key
     * @param msg
     * @param callback
     */
    public void send(String key, String msg, Callback callback);

    /**
     * 异步发送消息
     * 
     * @param partition
     * @param key
     * @param msg
     * @param callback
     */
    public void send(int partition, String key, String msg, Callback callback);

    /**
     * 获取一个消息生产者，可以控制各种发送
     */
    public Producer<String, byte[]> getProducer();

    /**
     * 获取一个事务性的生产者
     * 
     * @return
     */
    public Producer<String, byte[]> getTransProducer();

    public void close();

    /**
     * 将消息发送到broker
     */
    public void flush();

}

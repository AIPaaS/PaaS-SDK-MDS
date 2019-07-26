package com.ai.paas.ipaas.mds;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Processor {

    /**
     * 此处会在每收到一条消息时被调用，因此会很频繁，不要在这里进行初始化或者
     * 
     * @throws Exception
     */
    public void process(ConsumerRecord<String, String> message) throws Exception;

    /**
     * 处理一批消息
     * 
     * @param messages
     * @throws Exception
     */
    public void process(ConsumerRecords<String, String> consumerRecords) throws Exception;

    /**
     * 指示是否应用批量模式
     * 
     * @return
     */
    public boolean isBatch();

    /**
     * 获取批量大小
     * 
     * @return
     */
    public int getBatchSize();
}

package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.vo.MessageAndMetadata;

public interface IMessageProcessor {

    /**
     * 此处会在每收到一条消息时被调用，因此会很频繁，不要在这里进行初始化或者
     * 
     * @throws Exception
     */
    public void process(MessageAndMetadata message) throws Exception;

    /**
     * 处理一批消息
     * 
     * @param messages
     * @throws Exception
     */
    public void process(MessageAndMetadata[] messages) throws Exception;

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

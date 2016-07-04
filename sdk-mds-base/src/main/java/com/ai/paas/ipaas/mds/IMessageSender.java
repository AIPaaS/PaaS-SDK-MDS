package com.ai.paas.ipaas.mds;

public interface IMessageSender {

	/**
	 * 发送一条字符串消息
	 * 
	 * @param msg
	 *            消息体
	 * @param partitionKey
	 *            分区id，模取余
	 */
	public void send(String msg, long partitionKey);

	/**
	 * 发送字节消息
	 * 
	 * @param msg
	 *            消息体字节数组
	 * @param partitionKey
	 *            分区id，模取余
	 */
	public void send(byte[] msg, long partitionKey);

	/**
	 * 发送一条字符串消息
	 * 
	 * @param msg
	 *            消息体
	 * @param partitionKey
	 *            分区id，模取余
	 * @param key
	 *            消息的key
	 */
	public void send(String msg, long partitionKey, String key);

	/**
	 * 发送字节消息
	 * 
	 * @param msg
	 *            消息体字节数组
	 * @param partitionKey
	 *            分区id，模取余
	 * @param key
	 *            消息的key
	 */
	public void send(byte[] msg, long partitionKey, String key);
	
	/**
	 * 获取分区数量
	 * @return
	 */
	public int getParititions();
}

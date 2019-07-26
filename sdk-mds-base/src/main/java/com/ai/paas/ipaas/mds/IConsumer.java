package com.ai.paas.ipaas.mds;


public interface IConsumer {
	
	
	/**
	 * 启动消费端
	 */
	public void start();
	
	/**
	 * 停止消费端
	 */
	public void stop();
}

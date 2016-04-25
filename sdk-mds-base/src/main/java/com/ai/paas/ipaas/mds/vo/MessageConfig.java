package com.ai.paas.ipaas.mds.vo;

import java.io.Serializable;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class MessageConfig implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4646056511435799650L;

	public MessageConfig() {

	}

	private Properties kafaProps = new Properties();
	private GenericObjectPoolConfig poolConfig = null;
	private String userId = null;

	public MessageConfig(Properties kafkaCfg,
			GenericObjectPoolConfig poolConfig, String userId) {
		this.kafaProps.putAll(kafkaCfg);
		this.poolConfig = poolConfig;
		this.userId = userId;
	}

	public Properties getKafaProps() {
		return kafaProps;
	}

	public void setKafaProps(Properties kafaProps) {
		this.kafaProps = kafaProps;
	}

	public GenericObjectPoolConfig getPoolConfig() {
		return poolConfig;
	}

	public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

}

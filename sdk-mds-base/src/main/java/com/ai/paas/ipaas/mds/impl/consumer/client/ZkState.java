
package com.ai.paas.ipaas.mds.impl.consumer.client;

import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class ZkState implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8115258166361975256L;

	private static transient final Logger logger = LoggerFactory
			.getLogger(ZkState.class);

	transient ZKClient _zkClient;

	@SuppressWarnings({ "rawtypes" })
	private ZKClient newZKClient(Map stateConf) throws Exception {
		return new ZKClient(
				(String) stateConf.get(Config.KAFKA_ZOOKEEPER_HOSTS), 60000,
				"digest", stateConf.get(Config.KAFKA_ZOOKEEPER_USER) + ":"
						+ stateConf.get(Config.KAFKA_ZOOKEEPER_USER_PASSWD));
	}

	public ZKClient getZKClient() {
		assert _zkClient != null;
		return _zkClient;
	}

	public ZkState(KafkaConfig config) {

		try {
			_zkClient = newZKClient(config._stateConf);
			logger.info("Starting zkclient service");
		} catch (Exception e) {
			logger.error("Curator service not started");
			throw new RuntimeException(e);
		}
	}

	public ZkState(String connectionStr, String auth) {

		try {
			_zkClient = new ZKClient(connectionStr, 60000, "digest", auth);
			logger.info("Starting zkclient service:" + connectionStr);
		} catch (Exception e) {
			logger.error("Curator service not started");
			throw new RuntimeException(e);
		}
	}

	public void writeJSON(String path, Map<Object, Object> data) {
		logger.info("Writing " + path + " the data " + data.toString());
		Gson gson = new Gson();
		writeBytes(path, gson.toJson(data));
	}

	public void writeBytes(String path, String data) {
		try {
			if (!_zkClient.exists(path)) {
				_zkClient.createNode(path, data);
			} else {
				_zkClient.setNodeData(path, data);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public JsonObject readJSON(String path) {
		try {
			byte[] b = readBytes(path);
			if (b == null) {
				return null;
			}
			return new Gson().fromJson(
					new String(b, PaaSConstant.CHARSET_UTF8), JsonObject.class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] readBytes(String path) {
		try {
			if (_zkClient.exists(path)) {
				return _zkClient.getNodeBytes(path, null);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		if (null != _zkClient) {
			_zkClient.quit();
			_zkClient = null;
		}
	}
}

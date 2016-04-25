
package com.ai.paas.ipaas.mds.impl.consumer.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConfig implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 7801307286333846437L;
    public int _fetchSizeBytes = 1024 * 1024;
    public int _fillFreqMs = 200;

    public int _bufferSizeBytes = _fetchSizeBytes;

    public int _refreshFreqSecs = 100;
    public int _socketTimeoutMs = 60000;

    public boolean _forceFromStart = false;
    public long _startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
    public boolean _useStartOffsetTimeIfOffsetOutOfRange = true;

    public long _stateUpdateIntervalMs = 20000;
    @SuppressWarnings("rawtypes")
    public Map _stateConf;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public KafkaConfig(Properties props) {

        String zkHost = props.getProperty("kafka.zookeeper.hosts");
        String kafkaTopic = props.getProperty("kafka.topic");
        String brokerZkPath = props.getProperty("kafka.zookeeper.broker.path");
        String kafkaZkUser = props.getProperty("kafka.zookeeper.user");
        String kafkaZkUserPasswd = props
                .getProperty("kafka.zookeeper.user.passwd");

        String consumerZkPath = props.getProperty("zookeeper.consumer.path");
        String consumerConnection = props
                .getProperty("zookeeper.consumer.connection");
        String consumerUser = props.getProperty("zookeeper.consumer.user");
        String consumerUserPasswd = props
                .getProperty("zookeeper.consumer.user.passwd");
        String consumerId = props.getProperty("kafka.consumer.id");
        String decoderClass = props.getProperty("target.indexer.class");
        String mdsSrvId = props.getProperty(Config.MDS_USER_SRV_ID);

        // need to check the configuration values
        String partitionRunningLockPath = props.getProperty(Config.MDS_PARTITION_RUNNING_LOCK_PATH);
        if (partitionRunningLockPath == null || partitionRunningLockPath.length() == 0){
            throw new IllegalArgumentException("Failed to find the configuration [" +
                    Config.MDS_PARTITION_RUNNING_LOCK_PATH + "]");
        }
        String partitionPauseLockPath = props.getProperty(Config.MDS_PARTITION_PAUSE_LOCK_PATH);
        if (partitionPauseLockPath == null || partitionPauseLockPath.length() == 0){
            throw new IllegalArgumentException("Failed to find the configuration [" +
                    Config.MDS_PARTITION_PAUSE_LOCK_PATH + "]");
        }
        String offsetBasePath = props.getProperty(Config.MDS_PARTITION_OFFSET_BASE_PATH);
        if (offsetBasePath == null || offsetBasePath.length() == 0){
            throw new IllegalArgumentException("Failed to find the configuration [" +
                    Config.MDS_PARTITION_OFFSET_BASE_PATH + "]");
        }
        String consumerBasePath = props.getProperty(Config.MDS_CONSUMER_BASE_PATH);
        if (consumerBasePath == null || consumerBasePath.length() == 0){
            throw new IllegalArgumentException("Failed to find the configuration [" +
                    Config.MDS_CONSUMER_BASE_PATH + "]");
        }

        _stateConf = new HashMap();
        _stateConf.put(Config.KAFKA_ZOOKEEPER_HOSTS, zkHost);
        _stateConf.put(Config.KAFKA_TOPIC, kafkaTopic);
        _stateConf.put(Config.KAFKA_ZOOKEEPER_BROKER_PATH, brokerZkPath);
        _stateConf.put(Config.KAFKA_ZOOKEEPER_USER, kafkaZkUser);
        _stateConf.put(Config.KAFKA_ZOOKEEPER_USER_PASSWD, kafkaZkUserPasswd);

        _stateConf.put(Config.ZOOKEEPER_CONSUMER_PATH, consumerZkPath);
        _stateConf
                .put(Config.ZOOKEEPER_CONSUMER_CONNECTION, consumerConnection);
        _stateConf.put(Config.ZOOKEEPER_CONSUMER_USER, consumerUser);
        _stateConf.put(Config.ZOOKEEPER_CONSUMER_USER_PASSWD,
                consumerUserPasswd);
        _stateConf.put(Config.KAFKA_CONSUMER_ID, consumerId);
        _stateConf.put(Config.TARGET_INDEXER_CLASS, decoderClass);
        _stateConf.put(Config.MDS_USER_SRV_ID, mdsSrvId);

        //add mds config
        _stateConf.put(Config.MDS_PARTITION_RUNNING_LOCK_PATH, partitionRunningLockPath);
        _stateConf.put(Config.MDS_PARTITION_PAUSE_LOCK_PATH, partitionPauseLockPath);
        _stateConf.put(Config.MDS_PARTITION_OFFSET_BASE_PATH, offsetBasePath);
        _stateConf.put(Config.MDS_CONSUMER_BASE_PATH, consumerBasePath);
    }

}

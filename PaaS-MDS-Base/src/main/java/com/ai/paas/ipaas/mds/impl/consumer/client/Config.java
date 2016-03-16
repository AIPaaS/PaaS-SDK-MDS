/*
 *   This file is based on the source code of the Kafka spout of the Apache Storm project.
 *   (https://github.com/apache/storm/tree/master/external/storm-kafka)
 */

package com.ai.paas.ipaas.mds.impl.consumer.client;

import java.io.Serializable;
import java.util.HashMap;

public class Config extends HashMap<String, Object> implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 4813999257600271269L;
    /**
     * Kafka related configurations
     */
    public static final String KAFKA_ZOOKEEPER_HOSTS = "kafka.zookeeper.hosts";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String KAFKA_ZOOKEEPER_BROKER_PATH = "kafka.zookeeper.broker.path";
    public static final String KAFKA_ZOOKEEPER_USER = "kafka.zookeeper.user";
    public static final String KAFKA_ZOOKEEPER_USER_PASSWD = "kafka.zookeeper.user.passwd";


    /**
     * Consumer related configurations
     */
    public static final String ZOOKEEPER_CONSUMER_PATH = "zookeeper.consumer.path";
    public static final String ZOOKEEPER_CONSUMER_CONNECTION = "zookeeper.consumer.connection";
    public static final String ZOOKEEPER_CONSUMER_USER = "zookeeper.consumer.user";
    public static final String ZOOKEEPER_CONSUMER_USER_PASSWD = "zookeeper.consumer.user.passwd";
    public static final String KAFKA_CONSUMER_ID = "kafka.consumer.id";
    public static final String TARGET_INDEXER_CLASS = "target.indexer.class";

    /**
     * MDS related configurations
     */
    public static final String MDS_PARTITION_RUNNING_LOCK_PATH = "mds.partition.runninglock.path";
    public static final String MDS_PARTITION_PAUSE_LOCK_PATH = "mds.partition.pauselock.path";
    public static final String MDS_PARTITION_OFFSET_BASE_PATH = "mds.partition.offset.basepath";
    public static final String MDS_CONSUMER_BASE_PATH = "mds.consumer.base.path";


    public static final String MDS_USER_SRV_ID = "mds.user.srv.id";

}

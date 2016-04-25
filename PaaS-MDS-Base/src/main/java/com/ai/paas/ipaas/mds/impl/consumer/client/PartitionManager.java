
package com.ai.paas.ipaas.mds.impl.consumer.client;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.PaasRuntimeException;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcher;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcher.Event.KeeperState;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcherEvent;
import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.ai.paas.ipaas.mds.MsgConstant;
import com.ai.paas.ipaas.mds.vo.MessageAndMetadata;
import com.ai.paas.ipaas.util.StringUtil;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import kafka.api.OffsetRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("serial")
public class PartitionManager implements Serializable {
    private static transient final Logger logger = LoggerFactory
            .getLogger(PartitionManager.class);
    Long _emittedToOffset = -1L;
    Long _lastComittedOffset = -1L;
    Long _lastEnquedOffset = -1L;
    Long _startOffset;
    Long adjOffset = -1L;
    LinkedList<MessageAndOffset> _waitingToEmit = new LinkedList<MessageAndOffset>();
    Partition _partition;
    KafkaConfig _kafkaconfig;
    String _consumerId;
    transient SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    private ZKClient _zkClient = null;
    String _topic;
    @SuppressWarnings("rawtypes")
    Map _stateConf;
    Long _lastCommitMs = 0l;
    KafkaConsumer _receiver;
    boolean _restart;
    Long _lastFillTime = null;
    int delayTime = 1;
    int fetchInterval = 1;
    boolean refreshed = false;

    private ConfigWatcher contentWatcher = new ConfigWatcher() {
        public void processEvent(ConfigWatcherEvent event) {
            if (event == null) {
                // 不做什么
                return;
            }
            // 连接状态
            ConfigWatcher.Event.KeeperState keeperState = event.getState();
            // 事件类型
            ConfigWatcher.Event.EventType eventType = event.getType();
            if (ConfigWatcher.Event.EventType.NodeDataChanged == eventType) {
                // 监控到孩子变化，实现算法
                if (logger.isInfoEnabled()) {
                    logger.info("Get offset adjust chanage event, start to use new offset....!");
                }
                setNewOffset();
            }
            // 可能存在接收到其他事件，比如断开了
            if (KeeperState.Disconnected == keeperState
                    || ConfigWatcher.Event.KeeperState.Expired == keeperState) {
                // 断开了，由于是临时节点，因此不管了。其他应该可以增加
            }
        }

    };

    private void setNewOffset() {
        // 获取新的offset，继续监控
        try {
            String offset = _zkClient.getNodeData(getAdOffsetPath(), contentWatcher);
            if (!StringUtil.isBlank(offset)) {
                // 不为空，转换成整数
                try {
                    adjOffset = Long.parseLong(offset);
                    if (adjOffset >= 0) {
                        // 新的
                        _startOffset = adjOffset + 1;
                        // 此处设置一下新的commitAddr
                        refreshed = true;
                        logger.info("Get new offset for parition[" + _partition
                                + "] of topic " + _topic + "is:" + _startOffset);
                    }
                } catch (Exception e) {
                    logger.warn("Can not parse offset: " + offset, e);
                }
            }
        } catch (Exception e) {
            logger.warn("Error reading and/or parsing at ZkNode: "
                    + getAdOffsetPath(), e);
        }
    }

    public PartitionManager(DynamicPartitionConnections connections,
                            ZKClient zkClient, KafkaConfig kafkaconfig,
                            Partition partiionId, KafkaConsumer receiver, boolean restart) {
        _partition = partiionId;
        _connections = connections;
        _kafkaconfig = kafkaconfig;
        _stateConf = _kafkaconfig._stateConf;
        _consumerId = (String) _stateConf.get(Config.KAFKA_CONSUMER_ID);
        _consumer = connections.register(partiionId.host, partiionId.partition);
        _zkClient = zkClient;
        _topic = (String) _stateConf.get(Config.KAFKA_TOPIC);
        _receiver = receiver;
        _restart = restart;
        _startOffset = -1L;

        String consumerJsonId = null;
        Long jsonOffset = null;
        String path = getCommittedPath();
        Gson gson = new Gson();
        try {
            if (!_zkClient.exists(getAdOffsetPath())) {
                // 写入一个节点，以便观察
                _zkClient.createNode(getAdOffsetPath(), "-1");
            } else {
                // 和记录的位置比较一下，看看是否比其大，如果大，先记录上去
                try {
                    String offset = _zkClient.getNodeData(getAdOffsetPath());
                    adjOffset = Long.parseLong(offset);
                    JsonObject json = gson.fromJson(
                            _zkClient.getNodeData(path),
                            JsonObject.class);
                    if (json != null) {
                        consumerJsonId = json.get("consumer").getAsJsonObject()
                                .get("id").getAsString();
                        long sucOffset = json.get("offset").getAsLong();
                        if (adjOffset > sucOffset) {
                            // 设置值
                            _lastEnquedOffset = adjOffset;
                            _lastComittedOffset = adjOffset;
                            _emittedToOffset = adjOffset;
                            commit(null);
                        }
                    }
                } catch (Exception e) {
                    logger.error(getAdOffsetPath(), e);
                }
            }
            // 开始观察
            zkClient.getNodeData(getAdOffsetPath(),
                    contentWatcher);
            JsonObject json = gson.fromJson(
                    _zkClient.getNodeData(path),
                    JsonObject.class);
            logger.info("Read partition information from: " + path + "  --> "
                    + json);

            if (json != null) {
                consumerJsonId = json.get("consumer").getAsJsonObject()
                        .get("id").getAsString();
                jsonOffset = json.get("offset").getAsLong();
            }

        } catch (Throwable e) {
            logger.warn("Error reading and/or parsing at ZkNode: " + path, e);
        }

        if (consumerJsonId == null || jsonOffset == null) { // failed to
            // parse JSON?
            _lastComittedOffset = KafkaUtils.getOffset(_consumer, _topic,
                    partiionId.partition, kafkaconfig);
            logger.info("No partition information found, using configuration to determine offset");
        } else if (!_stateConf.get(Config.KAFKA_CONSUMER_ID).equals(
                consumerJsonId)
                && kafkaconfig._forceFromStart) {
            _lastComittedOffset = KafkaUtils.getOffset(_consumer, _topic,
                    partiionId.partition, kafkaconfig._startOffsetTime);
            logger.info("Topologgery change detected and reset from start forced, using configuration to determine offset");
        } else {

            _lastComittedOffset = jsonOffset;

            logger.info("Read last commit offset from zookeeper: "
                    + _lastComittedOffset + "; old topologgery_id: "
                    + consumerJsonId + " - new consumer_id: " + _consumerId);
        }

        logger.info("Starting Kafka " + _consumer.host() + ":"
                + partiionId.partition + " from offset " + _lastComittedOffset);
        _lastEnquedOffset = _lastComittedOffset;
        if (jsonOffset == _lastComittedOffset && _lastComittedOffset != 0) {
            _startOffset = _lastComittedOffset + 1;
            _emittedToOffset = _lastComittedOffset;
        } else {
            _startOffset = _lastComittedOffset;
            _emittedToOffset = _lastComittedOffset - 1;
        }

    }

    public void next() {
        if (_waitingToEmit.isEmpty()) {

            if (_lastFillTime == null
                    || (System.currentTimeMillis() - _lastFillTime) > _kafkaconfig._fillFreqMs) {
                logger.debug("_waitingToEmit is empty for topic " + _topic
                        + " for partition " + _partition.partition
                        + ".. Filling it every " + _kafkaconfig._fillFreqMs
                        + " miliseconds");
                fill();
                // 有东西了
                refreshed = false;
                _lastFillTime = System.currentTimeMillis();
            }

        }

        while (!refreshed) {
            MessageAndOffset msgAndOffset = _waitingToEmit.pollFirst();
            if (msgAndOffset != null) {
                Long key = msgAndOffset.offset();
                Message msg = msgAndOffset.message();
                try {
                    _lastEnquedOffset = key;
                    // 先设置为当前的，下面成功后设置为下一个
                    _startOffset = key;
                    logger.info("Entering the message circle...._lastEnquedOffset:"
                            + _lastEnquedOffset
                            + ",_lastComittedOffset:"
                            + _lastComittedOffset);
                    if (_lastEnquedOffset >= _lastComittedOffset) {
                        if (msg.payload() != null) {
                            MessageAndMetadata mmeta = new MessageAndMetadata();
                            mmeta.setTopic(_topic);
                            mmeta.setOffset(_lastEnquedOffset);
                            mmeta.setPartitionId(_partition.getId());
                            byte[] payload = new byte[msg.payload().remaining()];
                            msg.payload().get(payload);
                            mmeta.setMessage(payload);

                            if (msg.hasKey()) {
                                byte[] keyload = new byte[msg.key().remaining()];
                                msg.key().get(keyload);
                                mmeta.setKey(keyload);
                            }
                            // 此处相当于把消息给了消费端,如果没异常，就提交了。
                            _receiver.process(mmeta);
                            // 这里记录最后一次成功消费的位置
                            _emittedToOffset = key;
                            // 设置为下一条
                            _startOffset = msgAndOffset.nextOffset();
                            logger.info("Last offset for topic " + _topic
                                    + " for partition " + _partition.partition
                                    + " is : " + _emittedToOffset);

                        }
                    }
                } catch (Exception e) {
                    // 这里出错了也要提交一下位置
                    if (null != msg && null != msg.payload()) {
                        _lastEnquedOffset = key;
                        commit(e);
                    }
                    logger.error("Process Failed for offset " + key + " for  "
                            + _partition + " for topic " + _topic
                            + " with Exception" + e.getMessage());
                    try {
                        // 此处出了异常不能频繁重试，按指数级增加
                        TimeUnit.SECONDS.sleep(delayTime);
                        delayTime = 2 * delayTime;
                        if (delayTime >= 256) {
                            delayTime = 1;
                        }
                    } catch (InterruptedException e1) {
                        // 中断异常
                    }
                    // 需要异常抛出，不进行下面的操作
                    throw new PaasRuntimeException("Process Failed for offset "
                            + key + " for  " + _partition + " for topic "
                            + _topic, e);
                }
            } else {
                try {
                    if (refreshed && _lastComittedOffset > _emittedToOffset) {
                        _emittedToOffset = _lastComittedOffset;
                    }
                    commit(null);
                } catch (Exception e) {
                    _emittedToOffset = _lastComittedOffset;
                    _receiver.reportError("Error While Store for Partition "
                            + _partition, e);
                }
                break;
            }
        }

        // 退出while循环，此时应提交一下
        try {
            if (refreshed && _lastComittedOffset > _emittedToOffset) {
                _emittedToOffset = _lastComittedOffset;
            }
            commit(null);
        } catch (Exception e) {
            _emittedToOffset = _lastComittedOffset;
            _receiver.reportError("Error While Store for Partition "
                    + _partition, e);
        }
    }

    private void fill() {

        try {
            // 此处多取了一条已经消费过的
            FetchResponse fetchResponse = KafkaUtils.fetchMessages(
                    _kafkaconfig, _consumer, _partition, _startOffset);

            String topic = (String) _kafkaconfig._stateConf
                    .get(Config.KAFKA_TOPIC);
            logger.info("Fetching data from partition[" + _partition.partition
                    + "] for topic [" + topic + "] offset:" + _startOffset);
            if (fetchResponse.hasError()) {
                KafkaError error = KafkaError.getError(fetchResponse.errorCode(
                        topic, _partition.partition));
                if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE)
                        && _kafkaconfig._useStartOffsetTimeIfOffsetOutOfRange) {
                    long startOffset = KafkaUtils
                            .getOffset(_consumer, topic, _partition.partition,
                                    _kafkaconfig._startOffsetTime);
                    if (_startOffset < startOffset) {
                        // 只有当要取的offset还小于可用的时候，重置到大的值
                        logger.warn("Got fetch request with offset out of range: ["
                                + _emittedToOffset
                                + "]; for Topic"
                                + topic
                                + " ."
                                + "retrying with default start offset time from configuration. "
                                + "configured latest offset time: ["
                                + _kafkaconfig._startOffsetTime
                                + "] offset: ["
                                + startOffset + "]");
                        _startOffset = startOffset;
                    }
                    logger.warn("Retrying to fetch again from offset for topic "
                            + topic
                            + " from offset, partition: "
                            + _partition.partition
                            + " from offset:"
                            + _startOffset);

                    fetchResponse = KafkaUtils.fetchMessages(_kafkaconfig,
                            _consumer, _partition, _startOffset);

                } else {
                    String message = "Error fetching data from partition["
                            + _partition.partition + "] for topic [" + topic
                            + "]: [" + error + "]";
                    logger.error(message);
                    throw new FailedFetchException(message);
                }
            }

            ByteBufferMessageSet msgs = fetchResponse.messageSet(topic,
                    _partition.partition);
            for (MessageAndOffset msg : msgs) {
                if (msg.message() != null) {
                    _waitingToEmit.add(msg);
                }
            }
            if (_waitingToEmit.size() >= 1) {
                logger.info("Total " + _waitingToEmit.size()
                        + " messages from Kafka: " + _consumer.host() + ":"
                        + _partition.partition + " there in internal buffers");
            } else {
                // 此处相当于没有获取到消息，sleep一下
                try {
                    TimeUnit.SECONDS.sleep(fetchInterval);
                    fetchInterval = fetchInterval * 2;
                    if (fetchInterval >= 32) {
                        fetchInterval = 1;
                    }
                } catch (InterruptedException e1) {
                }
            }
        } catch (FailedFetchException fe) {
            logger.error("Exception during fill " + fe.getMessage());
            throw fe;
        }
    }

    private String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

    public void commit(Exception e) {

        logger.debug("LastComitted Offset : " + _lastComittedOffset);
        logger.debug("New Emitted Offset : " + _emittedToOffset);
        logger.debug("Enqueued Offset :" + _lastEnquedOffset);
        if (adjOffset > _emittedToOffset) {
            _lastEnquedOffset = adjOffset;
            _lastComittedOffset = adjOffset;
            _emittedToOffset = adjOffset;
            adjOffset = -1L;
        }
        if (_lastEnquedOffset >= _lastComittedOffset) {
            logger.debug("Committing offset for " + _partition);
            Map<Object, Object> data = new HashMap<Object, Object>();
            data.put("consumer", ImmutableMap.of("id", _consumerId));
            data.put("offset", _emittedToOffset);
            data.put("partition", _partition.partition);
            data.put("broker", ImmutableMap.of("host", _partition.host.host,
                    "port", _partition.host.port));
            data.put("topic", _topic);
            if (null != e) {
                data.put("message", e.toString());
                data.put("stacktrace", getStackTrace(e));
            }
            try {
                // 记录offset
                //采用新的ZKClient，需要注意以前的逻辑，以前会判断节点是否存在，如果存在，则修改
//                _zkClient.createNode(getCommittedPath(), new Gson().toJson(data));
                if (_zkClient.exists(getCommittedPath())){
                    _zkClient.setNodeData(getCommittedPath(), new Gson().toJson(data));
                }else{
                    _zkClient.createNode(getCommittedPath(), new Gson().toJson(data));
                }

                logger.debug("Wrote committed offset to ZK: "
                        + _emittedToOffset);
                _waitingToEmit.clear();
                _lastComittedOffset = _emittedToOffset;
            } catch (Exception zkEx) {
                // 此处相当于消费成功，但没记录offset成功
                logger.error("Error during commit. Let wait for refresh ", zkEx);
            }

            logger.debug("Committed offset " + _lastComittedOffset + " for "
                    + _partition + " for consumer: " + _consumerId);
        } else {
            logger.debug("Last Enqueued offset " + _lastEnquedOffset
                    + " not incremented since previous Comitted Offset "
                    + _lastComittedOffset + " for partition  " + _partition
                    + " for Consumer " + _consumerId
                    + ". Some issue in Process!!");
        }
    }

    private String getAdOffsetPath() {
        return _kafkaconfig._stateConf.get(Config.MDS_PARTITION_OFFSET_BASE_PATH)
                + PaaSConstant.UNIX_SEPERATOR + _partition.getId()
                + PaaSConstant.UNIX_SEPERATOR
                + MsgConstant.CONSUMER_ADJUSTED_OFFSET;
    }

    private String getCommittedPath() {
        return _kafkaconfig._stateConf.get(Config.MDS_PARTITION_OFFSET_BASE_PATH)
                + PaaSConstant.UNIX_SEPERATOR + _partition.getId();
    }

    public long queryPartitionOffsetLatestTime() {
        return KafkaUtils.getOffset(_consumer, _topic, _partition.partition,
                OffsetRequest.LatestTime());
    }

    public long lastCommittedOffset() {
        return _lastComittedOffset;
    }

    public Partition getPartition() {
        return _partition;
    }

    public void close() {
        try {

            _connections.unregister(_partition.host, _partition.partition);
            logger.info("Closed connection" + " for " + _partition);

        } catch (Exception ex) {
            logger.error("Error closing connection" + " for " + _partition);
        }

    }
}

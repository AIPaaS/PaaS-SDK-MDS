
package com.ai.paas.ipaas.mds.impl.consumer.client;

import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.ai.paas.ipaas.mds.IMessageProcessor;
import com.ai.paas.ipaas.mds.MessageClientException;
import com.ai.paas.ipaas.mds.impl.consumer.MessageConsumer;
import com.ai.paas.ipaas.mds.vo.MessageAndMetadata;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class KafkaConsumer implements Runnable, Serializable {
    private static transient final Logger logger = LoggerFactory
            .getLogger(KafkaConsumer.class);
    private static final long serialVersionUID = 1780042755212645597L;

    private final KafkaConfig _kafkaConfig;
    private int _partitionId;
    private IMessageProcessor _processor;
    private PartitionCoordinator _coordinator;
    private DynamicPartitionConnections _connections;
    private ZkState _state;
    private boolean _stopped = true;
    private boolean _pausing = true;
    private boolean _paused = true;
    private boolean _running = false;
    private ZKClient _zkClient = null;
    private MessageConsumer _consumer;
    private String _runningLockPath;
    private String _pauseLockPath;
    private PartitionManager _partitionManager = null;
    /**
     * 自裁锁，如果获得则本线程可以退出运行获取消息，但不能退出，继续获取运行锁
     */
    private InterProcessLock pauseLock = null;
    /**
     * 运行锁，如果获取到了，则必须释放自裁锁
     */
    private InterProcessLock runningLock = null;

    public KafkaConsumer(MessageConsumer consumer, KafkaConfig kafkaConfig,
                         int partitionId, ZKClient zkClient, String runningLockPath,
                         String pauseLockPath, IMessageProcessor processor) {
        _kafkaConfig = kafkaConfig;
        _partitionId = partitionId;
        // 此处不应所有线程共用一个连接，这样的话某个线程退出了，则ZK并没有退出，导致节点变化并没有监控到
        // 由于CCS性质，好像没办法
        _zkClient = zkClient;
        _consumer = consumer;
        _processor = processor;
        _runningLockPath = runningLockPath;
        _pauseLockPath = pauseLockPath;
        _stopped = true;
        _running = false;
        _partitionManager = null;
    }

    public void open() {
        // 为了支持分布式，需要加上获取锁
        // 如果为了分布式运行，还需要考虑平衡
        // 第一个启动的先占用所有分区
        // 如果第二个进程来了，则第一个进行自裁，需要释放些分区给第二个
        // 如果第三个进来，第一二个进行自裁，释放资源，但至少保证一个在跑
        // 如果获取到锁，则设置正常运行
        try {
            runningLock = _zkClient.getInterProcessLock(_runningLockPath);
            pauseLock = _zkClient.getInterProcessLock(_pauseLockPath);
        } catch (Exception e) {
            throw new MessageClientException(
                    "KafkaConsumer open() get mutex lock error!", e);
        }

        try {
            // 非阻塞模式
            logger.info("Parition id:" + _partitionId
                    + ",Starting to acquire lock....." + _runningLockPath);
            // 这里需要多尝试几次，默认5次吧，每次休眠60秒
            boolean gotLock = false;
            int loop = 0;
            while (!gotLock && loop < 5) {
            	//尝试20秒内获取锁
                if (runningLock.acquire(20, TimeUnit.SECONDS)) {
                    logger.info("Parition id:" + _partitionId
                            + ",Got running lock....." + _runningLockPath);
                    _paused = false;
                    _pausing = false;
                    _stopped = false;
                    _running = false;
                    _consumer.addConumser(this);
                    _state = new ZkState(_kafkaConfig);
                    boolean inited = false;
                    while (!inited) {
                        try {
                            _connections = new DynamicPartitionConnections(
                                    _kafkaConfig, new ZkBrokerReader(
                                    _kafkaConfig, _state));
                            _coordinator = new ZkCoordinator(_connections,
                                    _kafkaConfig, _zkClient, _state,
                                    _partitionId, this, true);
                            _partitionManager = _coordinator
                                    .getMyManagedPartitions().get(0);
                            inited = true;
                        } catch (Exception e) {
                            logger.error(
                                    "Topic parition init error! _partitionId="
                                            + _partitionId + " error!", e);
                            TimeUnit.SECONDS.sleep(60);
                        }
                    }
                    // 释放暂停锁，这里不判断了
                    // 这里是JVM的会释放掉，是否一个jvm一个队列，一个分区，一个线程？
                    // 第一次运行没有这个锁，会报一个异常，不处理
                    try {
                        if (pauseLock.acquire(1, TimeUnit.SECONDS)) {
                            pauseLock.release();
                            try {
                                pauseLock.release();
                            } catch (Exception e) {
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Release pause lock:=" + _pauseLockPath
                                + " error!", e);
                    }
                    gotLock = true;
                } else {
                    // 如果没有获得，休眠60
                    TimeUnit.SECONDS.sleep(60);
                }
                loop++;
            }
        } catch (Exception e) {
            logger.error("Acquire running lock:=" + _runningLockPath
                    + " error!", e);
        }
        _consumer.addRunningNum();
    }

    /**
     * 处理每条消息时使用
     *
     * @param mmeta
     */
    public void process(MessageAndMetadata mmeta) {
        // 这里进行处理每个消息
        if (null != mmeta) {
            try {
                _processor.process(mmeta);
            } catch (Exception e) {
                throw new FailedFetchException(e);
            }
        }
    }

    public void reportError(String errMsg, Throwable e) {
        // 暂时不知道干啥
        logger.error(errMsg, e);
    }

    public void close() {
        commit();
        if (null != _state)
            _state.close();
        if (null != _connections)
            _connections.clear();
        _coordinator = null;
    }

    /**
     * 循环创建消息
     */
    public void createStream() {
        try {
            if (null != _partitionManager)
                _partitionManager.next();
        } catch (FailedFetchException fe) {
            // 这里可能存在kafka问题，也可能是自身的问题
            logger.warn("Fetch failed. Refresing Coordinator..", fe);
            try {
            	//再尝试取消息
                _coordinator.refresh();
                _partitionManager = _coordinator.getMyManagedPartitions()
                        .get(0);
            } catch (Exception ex) {
                logger.error("Error refresing Coordinator.. for partition id:"
                        + _partitionId, ex);
            }
        } catch (Exception ex) {
            logger.error("Partition " + _partitionId
                    + " encountered error during createStream : ", ex);
            // 此线程不能退出，需要重复尝试创建，万一存在其他异常，也不退出？
        }

    }

    public void deactivate() {
        commit();
    }

    private void commit() {
        if (null != _partitionManager)
            _partitionManager.commit(null);
    }

    @Override
    public void run() {
        open();
        _running = true;
        logger.info("Parition " + _partitionId + " is running.........");
        try {
            while (!_stopped && !_pausing) {
                this.createStream();
            }
        } catch (Throwable t) {
            logger.error("Parition " + _partitionId + " has error!", t);
        }
        logger.info("Start to stop the thread for topic:" + _partitionId);
        close();
        try {
            if (runningLock.acquire(1, TimeUnit.SECONDS)) {
                runningLock.release();
                try {
                    runningLock.release();
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
            logger.error("Release running lock:=" + _runningLockPath
                    + " error!", e);
        }
        // 此处应该完事了，这是设置pause=true
        _paused = true;
        _stopped = true;
        _running = false;
        _consumer.decreaseRunningNum();
        logger.error("Parition " + _partitionId + "---- stopped");
    }

    public boolean isPaused() {
        return _paused;
    }

    public void pause() {
        // 开始获取暂停锁，
        try {
            logger.info("Start to pause the thread for topic:" + _partitionId);
            if (pauseLock.acquire(120, TimeUnit.SECONDS)) {
                // 获取到锁了
                _pausing = true;
            }
            while (!isPaused()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
            try {
                pauseLock.release();
                try {
                    pauseLock.release();
                } catch (Exception e) {
                }
                logger.info("End to pause the thread for topic:" + _partitionId);
            } catch (Exception e) {
                logger.error("Release pausing lock error!", e);
            }
        } catch (Exception e) {
            logger.error("Acquire lock error!", e);
        }
    }

    public void stop() {
        _stopped = true;
    }

    public boolean isStopped() {
        return _stopped;
    }

    public boolean isRunning() {
        return _running;
    }

    public int getPartitionId() {
        return _partitionId;
    }

    @Override
    public String toString() {
        return _kafkaConfig._stateConf.get(Config.KAFKA_TOPIC) + ":"
                + _partitionId;
    }

}

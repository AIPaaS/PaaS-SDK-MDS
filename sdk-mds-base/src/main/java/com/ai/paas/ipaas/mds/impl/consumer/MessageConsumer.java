package com.ai.paas.ipaas.mds.impl.consumer;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.PaasRuntimeException;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcher;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcher.Event.KeeperState;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcherEvent;
import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.ai.paas.ipaas.mds.*;
import com.ai.paas.ipaas.mds.impl.consumer.client.*;
import com.ai.paas.ipaas.util.ResourceUtil;
import com.ai.paas.ipaas.util.UUIDTool;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageConsumer implements IMessageConsumer {
	private static transient final Logger logger = LoggerFactory
			.getLogger(MessageConsumer.class);
	/**
	 * 消费线程列表/每个分区一个
	 */
	private List<KafkaConsumer> consumers = Collections.synchronizedList(new ArrayList<KafkaConsumer>());
	/**
	 * 分区数
	 */
	private List<Integer> paritions = new ArrayList<>();
	/**
	 * 消息队列配置
	 */
	private KafkaConfig kafkaConfig;
	private IMsgProcessorHandler msgProcessorHandler;
	private String id = UUIDTool.genId32();
	private ZKClient zkClient = null;
	private int _partitionCount;
	private AtomicInteger curRunningParitions = new AtomicInteger(0);
	private IMessageProcessor[] processors = null;
	String runningLockPath = null;
	String pauseLockPath = null;
	String path = null;

	Thread.UncaughtExceptionHandler eh = new Thread.UncaughtExceptionHandler() {
		public void uncaughtException(Thread th, Throwable ex) {
			// 回头再处理
			logger.error(th.toString(), ex);
		}
	};

	private ConfigWatcher childWatcher = new ConfigWatcher() {
		public void processEvent(ConfigWatcherEvent event) {
			if (event == null) {
				// 不做什么
				return;
			}
			// 连接状态
			ConfigWatcher.Event.KeeperState keeperState = event.getState();
			// 事件类型
			ConfigWatcher.Event.EventType eventType = event.getType();
			if (ConfigWatcher.Event.EventType.NodeChildrenChanged == eventType) {
				// 监控到孩子变化，说明有新的进程启动或停止，实现算法
				if (logger.isInfoEnabled()) {
					logger.info("Get ZK node children num chanage event!");
				}
				// 开始分裂或者收缩各个线程的负责分区
				startStopping();
			}
			if (ConfigWatcher.Event.EventType.NodeDeleted == eventType) {
				// 这时应该是删除孩子节点节点了
				// 这是应该增加运行的分区
				startStopping();
			}
			// 可能存在接收到其他事件，比如断开了
			if (KeeperState.Disconnected == keeperState
					|| ConfigWatcher.Event.KeeperState.Expired == keeperState) {
				// 断开了，由于是临时节点，因此不管了。其他应该可以增加
			}
		}

	};

	public MessageConsumer(ZKClient zkClient, KafkaConfig kafkaConfig,
			IMsgProcessorHandler msgProcessorHandler) {
		// 先省略参数,// 采用传入模式,避免和CCS耦合
		this.zkClient = zkClient;
		this.kafkaConfig = kafkaConfig;
		this.msgProcessorHandler = msgProcessorHandler;

	}

	public MessageConsumer init() {

		// 获取所有分区数，还得初始化
		ZkState zkState = new ZkState(kafkaConfig);
		DynamicBrokersReader _reader = new DynamicBrokersReader(kafkaConfig,
				zkState);
		_partitionCount = _reader.getNumPartitions();
		logger.info("Get topic:"
				+ kafkaConfig._stateConf.get(Config.KAFKA_TOPIC) + " has "
				+ _partitionCount + " parittions!");
		processors = msgProcessorHandler.createInstances(_partitionCount);
		// 数量不匹配
		if (null == processors || processors.length < _partitionCount) {
			throw new PaasRuntimeException(
					ResourceUtil
							.getMessage("com.ai.paas.ipaas.msg.process_init_error"));
		}
		// 然后循环建立reciver
		runningLockPath = (kafkaConfig._stateConf
				.get(Config.MDS_PARTITION_RUNNING_LOCK_PATH) + PaaSConstant.UNIX_SEPERATOR);
		pauseLockPath = (kafkaConfig._stateConf
				.get(Config.MDS_PARTITION_PAUSE_LOCK_PATH) + PaaSConstant.UNIX_SEPERATOR);
		for (int i = 0; i < _partitionCount; i++) {
			// 这里需要初始化锁
			paritions.add(i);
		}

		return this;
	}

	public void addConumser(KafkaConsumer consumer) {
		consumers.add(consumer);
	}

	public void addRunningNum() {
		curRunningParitions.addAndGet(1);
	}

	public void decreaseRunningNum() {
		curRunningParitions.addAndGet(-1);
	}

	/**
	 * 注册运行的consumer
	 */
	private void registerConsumer() {
		// 注册一个应用节点，唯一标识,同时监控
		final String path = String.valueOf(kafkaConfig._stateConf
				.get(Config.MDS_CONSUMER_BASE_PATH));
		try {
			zkClient.createNode(path + "/" + id, ""
					+ Thread.currentThread().getId(), CreateMode.EPHEMERAL);
			zkClient.getChildren(path, childWatcher);
		} catch (Exception e) {
			throw new MessageClientException(
					"MessageConsumer register consumer!", e);
		}
	}

	private void startStopping() {
		// 注册一个应用节点，唯一标识,同时监控
		final String path = String.valueOf(kafkaConfig._stateConf
				.get(Config.MDS_CONSUMER_BASE_PATH));
		List<String> children;
		try {
			children = zkClient.getChildren(path, childWatcher);
		} catch (Exception e) {
			throw new MessageClientException("", e);
		}
		if (null != children) {
			int avgNum = calAvgRunningNum(children.size());
			stopRunningPartition(avgNum);
		}
	}

	public void stopRunningPartition(int avgNum) {
		// 计算出来是0，啥也不做
		if (avgNum <= 0)
			return;
		int currentRunNum = curRunningParitions.get();
		logger.info("Current running threads:" + currentRunNum
				+ ",Need to keep threads:" + avgNum);
		if (currentRunNum > 1 && currentRunNum > avgNum) {
			// 开始自裁，循环列表,调用自裁方法
			// 此时可能每个进程都在计算，且都满足自裁条件
			// 在一定时间内，自裁是否成功（自裁分布式锁）
			// 自裁成功后，释放占用锁，给其他进程占用，此时占用分布式锁，但要过一段时间获取占用锁，此时
			// 应该别的进程占用了，阻塞，如果别的进程宕机了，此时获得占用锁，释放自裁锁
			// 自裁不成功,没有自裁锁，退出本次
			int count = currentRunNum - avgNum;
			// 从后面开始停止
			Collections.reverse(consumers);
			Iterator<KafkaConsumer> iter = consumers.iterator();
			int i = 0;
			KafkaConsumer consumer = null;
			while (iter.hasNext() && i < count) {
				consumer = iter.next();
				if (!consumer.isPaused() && consumer.isRunning()) {
					pauseConsumer(consumer);
					i++;
				}
			}
			// 此处都已经停止了，等待10秒以便线程退出
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
		}
		currentRunNum = curRunningParitions.get();
		logger.info("Current running threads:" + currentRunNum
				+ ",Need to keep threads:" + avgNum);
		if (currentRunNum < avgNum) {
			// 这里表明有进程宕掉了，需要重新启动一些进程
			Collections.reverse(consumers);
			Iterator<KafkaConsumer> iter = consumers.iterator();
			Thread _consumerThread = null;
			KafkaConsumer consumer = null;
			while (iter.hasNext()) {
				// 这里都一遍，看谁抢到吧
				consumer = iter.next();
				logger.info("Partition id:" + consumer.getPartitionId()
						+ ", isPaused:" + consumer.isPaused() + ",isStopped:"
						+ consumer.isStopped());
				if (consumer.isPaused() && consumer.isStopped()) {
					// 这里需要新起一个新的实例，以前使用旧实例导致变量共享
					consumer = new KafkaConsumer(this, kafkaConfig,
							consumer.getPartitionId(), zkClient,
							runningLockPath + consumer.getPartitionId(),
							pauseLockPath + consumer.getPartitionId(),
							processors[consumer.getPartitionId()]);
					_consumerThread = new Thread(consumer);
					_consumerThread.setDaemon(true);
					_consumerThread.setUncaughtExceptionHandler(eh);
					_consumerThread.start();
				}
			}
		}

	}

	private void pauseConsumer(KafkaConsumer consumer) {
		// 先暂停，等待提交
		consumer.pause();
	}

	private int calAvgRunningNum(int consumers) {
		// 参数为目前起来的总进程数
		if (consumers <= 0)
			return 0;
		return (int) Math.ceil(_partitionCount * 1.0 / consumers);
	}

	public void start() {
		// 初始化
		init();
		// 开始运行
		Iterator<Integer> iter = paritions.iterator();

		Thread _consumerThread = null;
		KafkaConsumer kafkaConsumer = null;
		int k = 0;
		while (iter.hasNext()) {
			// 自己建立线程吧
			k = iter.next();
			kafkaConsumer = new KafkaConsumer(this, kafkaConfig, k, zkClient,
					runningLockPath + k, pauseLockPath + k, processors[k]);
			_consumerThread = new Thread(kafkaConsumer);
			_consumerThread.setDaemon(true);
			_consumerThread.setUncaughtExceptionHandler(eh);
			_consumerThread.start();
		}
		// 此时注册，避免过早获取了监听事件，又将同一实例作为线程又启动
		registerConsumer();
	}

	public void stop() {
		// 停止所有的线程？
		logger.info("Start to stopping all message consumers....");
		KafkaConsumer consumer = null;
		Iterator<KafkaConsumer> iter = consumers.iterator();
		while (iter.hasNext()) {
			consumer = iter.next();
			consumer.stop();
		}
	}
}

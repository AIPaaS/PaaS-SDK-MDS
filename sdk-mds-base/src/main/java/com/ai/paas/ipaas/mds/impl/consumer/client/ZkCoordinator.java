
package com.ai.paas.ipaas.mds.impl.consumer.client;

import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class ZkCoordinator implements PartitionCoordinator, Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 3917060598429258965L;

    private static transient final Logger logger = LoggerFactory
            .getLogger(ZkCoordinator.class);

    KafkaConfig _kafkaconfig;
    int _partitionOwner;
    @SuppressWarnings({"unchecked", "rawtypes"})
    Map<Partition, PartitionManager> _managers = new HashMap();
    List<PartitionManager> _cachedList;
    Long _lastRefreshTime = 0L;
    int _refreshFreqMs;
    DynamicPartitionConnections _connections;
    DynamicBrokersReader _reader;
    GlobalPartitionInformation _brokerInfo;
    KafkaConfig _config;
    KafkaConsumer _receiver;
    ZKClient _zkClient;
    boolean _restart;

    public ZkCoordinator(DynamicPartitionConnections connections,
                         KafkaConfig config, ZKClient zkClient, ZkState state,
                         int partitionId, KafkaConsumer receiver, boolean restart) {
        _kafkaconfig = config;
        _connections = connections;
        _partitionOwner = partitionId;
        _refreshFreqMs = config._refreshFreqSecs * 1000;
        _zkClient = zkClient;
        _reader = new DynamicBrokersReader(_kafkaconfig, state);
        _brokerInfo = _reader.getBrokerInfo();
        _config = config;
        _receiver = receiver;
        _restart = restart;

    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        if ((System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
            refresh();
            _lastRefreshTime = System.currentTimeMillis();
        }
        _restart = false;
        return _cachedList;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void refresh() {
        try {
            logger.info("Refreshing partition manager connections");
            _brokerInfo = _reader.getBrokerInfo();
            Set<Partition> mine = new HashSet();
            for (Partition partition : _brokerInfo) {
                if (partition.partition == _partitionOwner) {
                    mine.add(partition);
                    logger.info("Added partition index " + _partitionOwner
                            + " for coordinator");
                }
            }

            Set<Partition> curr = _managers.keySet();
            Set<Partition> newPartitions = new HashSet<Partition>(mine);
            newPartitions.removeAll(curr);

            Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
            deletedPartitions.removeAll(mine);

            logger.info("Deleted partition managers: "
                    + deletedPartitions.toString());

            for (Partition id : deletedPartitions) {
                PartitionManager man = _managers.remove(id);
                man.close();
            }
            logger.info("New partition managers: " + newPartitions.toString());

            for (Partition id : newPartitions) {

                PartitionManager man = new PartitionManager(
                        _connections,
                        _zkClient,
                        _kafkaconfig, id, _receiver, _restart);
                _managers.put(id, man);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        logger.info("Finished refreshing");
    }

    @Override
    public PartitionManager getManager(Partition partition) {
        return _managers.get(partition);
    }
}

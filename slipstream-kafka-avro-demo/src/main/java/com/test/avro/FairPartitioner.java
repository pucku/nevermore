package com.test.avro;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class FairPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes,
			Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();

		return Integer.valueOf((String) key) % numPartitions;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}

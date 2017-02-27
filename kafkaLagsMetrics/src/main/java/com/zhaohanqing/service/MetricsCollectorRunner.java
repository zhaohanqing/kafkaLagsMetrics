package com.zhaohanqing.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhaohanqing.model.ClusterMetrics;
import com.zhaohanqing.model.ConsumerGroupMetrics;
import com.zhaohanqing.model.KafkaCluster;
import com.zhaohanqing.model.PartitionMetrics;
import com.zhaohanqing.model.TopicMetrics;
import com.zhaohanqing.utils.UtilHelper;

import kafka.common.TopicAndPartition;

public class MetricsCollectorRunner implements Callable<ClusterMetrics>{
	private final Logger LOGGER = LoggerFactory.getLogger(MetricsCollectorRunner.class);
	private KafkaAdminConsumerClient kafkaAdminConsumerClient;
	private KafkaCluster thisCluster;
	private HashMap<String, ConsumerGroupMetrics> consumerGroupMetricsMap;
	private HashMap<String, HashMap<String, HashMap<Integer, String>>> consumerGroupTopicPartitionMap;
	
	
	public MetricsCollectorRunner(KafkaCluster cluster) {
		thisCluster = cluster;
		kafkaAdminConsumerClient = new KafkaAdminConsumerClient();
		String[] kafkaServerUrlsArray = cluster.getKafkaBootstrapServerURL().split(",");
		kafkaAdminConsumerClient.setMetricsKafkaServersArray(kafkaServerUrlsArray);
		kafkaAdminConsumerClient.setMetricsKafkaPort(Integer.valueOf(cluster.getPort()));
		consumerGroupMetricsMap = new HashMap<String, ConsumerGroupMetrics>();
		consumerGroupTopicPartitionMap = new HashMap<String, HashMap<String, HashMap<Integer, String>>>();	
	}
	
	
	/*
	 * Sync relationship of consumer group and topic and partition
	 * */
	public void syncConsumerGroupTopicPartitionCache(){
		if(null == kafkaAdminConsumerClient){
			return;
		}
		synchronized (consumerGroupTopicPartitionMap) {
			consumerGroupTopicPartitionMap.clear();
			HashMap<String, List<String>> consumerGroupAndTopicsMap = kafkaAdminConsumerClient.getGroupIdAndTopicsMapFromKafka();
			for(Map.Entry<String, List<String>>entry: consumerGroupAndTopicsMap.entrySet()){
				String groupId = entry.getKey();
				List<String> topicList = entry.getValue();
				HashMap<String, HashMap<Integer, String>> topicPartitionMap = new HashMap<String, HashMap<Integer, String>>();
				for(String topic:topicList){
					HashMap<Integer, String> partitionMap = kafkaAdminConsumerClient.getPartitionInfoMapFromKafka(groupId, topic);
					topicPartitionMap.put(topic, partitionMap);
				}
				consumerGroupTopicPartitionMap.put(groupId, topicPartitionMap);
			}
		}
	}
	
	public HashMap<String, ConsumerGroupMetrics> syncConsumerGroupMetricsMap(){
		if(null == kafkaAdminConsumerClient){
			return null;
		}
		synchronized(consumerGroupMetricsMap){
			consumerGroupMetricsMap.clear();
			HashMap<String, ConsumerGroupMetrics> consumerGroupetricsMap = new HashMap<String, ConsumerGroupMetrics>();
			List<String> groupIdList = kafkaAdminConsumerClient.getGroupIdListFromKafka();
			for(String groupId:groupIdList){
				ConsumerGroupMetrics cgm = kafkaAdminConsumerClient.getconsumerGroupMetricsSchema(groupId);
				consumerGroupMetricsMap.put(groupId, cgm);
			}
			return consumerGroupetricsMap;
		}
		
	}
	
	/*
	 * get log size
	 * */
	private void getLogSize(){
		if(null == kafkaAdminConsumerClient || consumerGroupTopicPartitionMap.isEmpty()){
			return;	
		}
		for(Map.Entry<String, HashMap<String, HashMap<Integer, String>>> entry:consumerGroupTopicPartitionMap.entrySet()){
			String groupId = entry.getKey();
			HashMap<String, HashMap<Integer, String>> topicPartitionMap = entry.getValue();
			loadConsumerGroupLogSize(groupId, topicPartitionMap);
		}
	}
	
	private HashMap<String, List<TopicAndPartition>> getTopicleaderPartitionMap(String topic, HashMap<Integer, String> partitionMap){
		HashMap<String, List<TopicAndPartition>> leaderHostAndTopicPartitionMap = new HashMap<String, List<TopicAndPartition>>();
		for(Map.Entry<Integer, String> entry:partitionMap.entrySet()){
			int partitionId = entry.getKey();
			String leaderHost = entry.getValue();
			TopicAndPartition tp = new TopicAndPartition(topic, partitionId);
			if(leaderHostAndTopicPartitionMap.containsKey(leaderHost)){
				leaderHostAndTopicPartitionMap.get(leaderHost).add(tp);
			}else{
				List<TopicAndPartition> topicAndPartitionList = new ArrayList<TopicAndPartition>();
				topicAndPartitionList.add(tp);
				leaderHostAndTopicPartitionMap.put(leaderHost, topicAndPartitionList);
			}
		}
		return leaderHostAndTopicPartitionMap;
	}
	
	private void loadTopicLogSize(String groupId, String topic, HashMap<Integer, String> partitionMap){
		HashMap<String, List<TopicAndPartition>> leaderHostAndTopicPartitionMap = getTopicleaderPartitionMap(topic, partitionMap);
		TopicMetrics tm = new TopicMetrics();
		tm.topic = topic;
		for(Map.Entry<String, List<TopicAndPartition>> entry:leaderHostAndTopicPartitionMap.entrySet()){
			String leaderHost = entry.getKey();
			List<TopicAndPartition> tpList = entry.getValue();
			HashMap<Integer, PartitionMetrics> partitionMetricsMap = kafkaAdminConsumerClient.getLogSize(groupId, tpList, leaderHost);			
			for(Map.Entry<Integer, PartitionMetrics> ent:partitionMetricsMap.entrySet()){
				int partitionId = ent.getKey();
				PartitionMetrics partitionMetrics = ent.getValue();
				Map<String, String> partitionValueMap = new HashMap<String, String>();
				partitionValueMap.put("LOGSIZE", Long.toString(partitionMetrics.getLogSize()));
				partitionValueMap.put("LEADERHOST", partitionMetrics.leaderHost);
				UtilHelper.fillConsumergroupMetrics(consumerGroupMetricsMap.get(groupId), topic, partitionId, partitionValueMap);
			}
		}
	}
	
	private void loadConsumerGroupLogSize(String groupId, HashMap<String, HashMap<Integer, String>> topicPartitionMap){
		for(Map.Entry<String, HashMap<Integer, String>> entry:topicPartitionMap.entrySet()){
			String topic = entry.getKey();
			HashMap<Integer, String> partitionMap = entry.getValue();
			loadTopicLogSize(groupId, topic, partitionMap);	
		}
	}
	
	/*
	 * get fetch offset
	 * */
	private void getOffset(){
		if(null == kafkaAdminConsumerClient || consumerGroupTopicPartitionMap.isEmpty()){
			return;	
		}
		for(Map.Entry<String, HashMap<String, HashMap<Integer, String>>> entry:consumerGroupTopicPartitionMap.entrySet()){
			String groupId = entry.getKey();
			HashMap<String, HashMap<Integer, String>> topicPartitionMap = entry.getValue();
			loadConsumerGroupOffset(groupId, topicPartitionMap);
		}
	}
	
	
	private void loadConsumerGroupOffset(String groupId, HashMap<String, HashMap<Integer, String>> topicPartitionMap){
		for(Map.Entry<String, HashMap<Integer, String>> entry:topicPartitionMap.entrySet()){
			String topic = entry.getKey();
			HashMap<Integer, String> partitionMap = entry.getValue();
			loadTopicOffset(groupId, topic, partitionMap);	
		}
	}
	
	private void loadTopicOffset(String groupId, String topic, HashMap<Integer, String> partitionMap){
		TopicMetrics tm = new TopicMetrics();
		tm.topic = topic;
		List<TopicAndPartition> topicAndPartitionList = new ArrayList<TopicAndPartition>();
		for(Map.Entry<Integer, String>entry : partitionMap.entrySet()){
			int partitionId = entry.getKey();
			TopicAndPartition tp = new TopicAndPartition(topic, partitionId);
			topicAndPartitionList.add(tp);
		}
		HashMap<Integer, PartitionMetrics> partitionOffsetMap = kafkaAdminConsumerClient.getFetchOffset(groupId, topicAndPartitionList);
		for(Map.Entry<Integer, PartitionMetrics> ent:partitionOffsetMap.entrySet()){
			int partitionId = ent.getKey();
			PartitionMetrics partitionMetrics = ent.getValue();
			Map<String, String> partitionValueMap = new HashMap<String, String>();
			partitionValueMap.put("OFFSET", Long.toString(partitionMetrics.getOffset()));
			UtilHelper.fillConsumergroupMetrics(consumerGroupMetricsMap.get(groupId), topic, partitionId, partitionValueMap);
		}
	}
	
	@Override
	public ClusterMetrics call() throws Exception {
		LOGGER.info("run MetricsCollectorRunner call for " + thisCluster.getClusterName());
		syncConsumerGroupTopicPartitionCache();
		syncConsumerGroupMetricsMap();
		getOffset();
		getLogSize();
		ClusterMetrics clusterMetrics = new ClusterMetrics();
		clusterMetrics.clusterName = thisCluster.getClusterName();
		clusterMetrics.ConsumerGroupMetricsMap = consumerGroupMetricsMap;
		kafkaAdminConsumerClient.close();
		LOGGER.info("close MetricsCollectorRunner call for " + thisCluster.getClusterName());
		return clusterMetrics;
	}
}

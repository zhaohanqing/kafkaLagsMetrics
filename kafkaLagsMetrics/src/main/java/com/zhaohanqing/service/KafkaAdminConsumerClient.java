package com.zhaohanqing.service;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhaohanqing.model.ConsumerGroupMetrics;
import com.zhaohanqing.model.PartitionMetrics;
import com.zhaohanqing.utils.UtilHelper;

import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerSummary;
import kafka.api.GroupCoordinatorRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.coordinator.GroupOverview;
import kafka.javaapi.GroupCoordinatorResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.network.BlockingChannel;
import scala.collection.JavaConversions;


public class KafkaAdminConsumerClient {
	private final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminConsumerClient.class);
	private SimpleConsumer simpleConsumer;
	private ZooKeeper zk;
	private AdminClient adminClient;
	private String[] metricsKafkaServersArray;
	private String[] metricsZookeeperServersArray;
	private int metricsKafkaPort;
	private int metricsZookeeperPort;
	private String metricsClientId = "KAFKALAGSMETRICS";
	private int connectTimeout = 10000;
	private int producerBufferSize = 65535;
	
	private Map<String, KafkaConsumer<String, String>> kafkaConsumerMap;
	private Map<String, SimpleConsumer> dynamicSimpleConsumerMap;
	
	public KafkaAdminConsumerClient(){
		kafkaConsumerMap = new HashMap<String, KafkaConsumer<String, String>>();
		dynamicSimpleConsumerMap = new HashMap<String, SimpleConsumer>();
	}
	
	private String getMetricsKafkaBootstrapServers(){
		StringBuffer sb = new StringBuffer();
		for(String s:metricsKafkaServersArray){
			sb.append(s);
			sb.append(":");
			sb.append(metricsKafkaPort);
			sb.append(",");
		}
		sb.substring(0, sb.length() -1);
		return sb.toString();
	}
	
	
	private  SimpleConsumer getDynamicConsumer(String leaderBrokerIP){
		if(dynamicSimpleConsumerMap.containsKey(leaderBrokerIP)){
			return dynamicSimpleConsumerMap.get(leaderBrokerIP);
		}else{
			SimpleConsumer sc = new SimpleConsumer(leaderBrokerIP, 
					metricsKafkaPort,
					connectTimeout,
					producerBufferSize, 
					metricsClientId);
			dynamicSimpleConsumerMap.put(leaderBrokerIP, sc);
			return sc;
		}
	}
	
	private  KafkaConsumer<String, String> createKafkaConsumer(String groupId){
		if(kafkaConsumerMap.containsKey(groupId)){
			return kafkaConsumerMap.get(groupId);
		}else{
			String KAFKA_METRICS_BOOTSTRAP_SERVERS = getMetricsKafkaBootstrapServers();
			Properties kafkaConsumerProps = new Properties();
			kafkaConsumerProps.put("group.id", groupId);
			kafkaConsumerProps.put("bootstrap.servers", KAFKA_METRICS_BOOTSTRAP_SERVERS);
			kafkaConsumerProps.put("auto.commit.interval.ms", "1000");
			kafkaConsumerProps.put("session.timeout.ms", connectTimeout);
			kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			kafkaConsumerProps.put("enable.auto.commit",false);
			KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(kafkaConsumerProps);
			kafkaConsumerMap.put(groupId, kafkaConsumer);
			return kafkaConsumer;
		}
	}
	
	private  AdminClient getAdminClient(){
		if(null != adminClient){
			return adminClient;
		}else{
			Time time = new SystemTime();
			Metrics metrics = new Metrics(time);
			Metadata metadata = new Metadata();
			ConfigDef configs = new ConfigDef();
			configs.define(
			        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
			        org.apache.kafka.common.config.ConfigDef.Type.LIST,
			        Importance.HIGH,
			        CommonClientConfigs.BOOSTRAP_SERVERS_DOC)
			      .define(
			        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
			        ConfigDef.Type.STRING,
			        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
			        ConfigDef.Importance.MEDIUM,
			        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
			      .withClientSslSupport()
			      .withClientSaslSupport();
			HashMap<String, String> originals = new HashMap<String, String>();
			String KAFKA_METRICS_BOOTSTRAP_SERVERS = getMetricsKafkaBootstrapServers();
			originals.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_METRICS_BOOTSTRAP_SERVERS);
			AbstractConfig abstractConfig = new AbstractConfig(configs, originals);
			ChannelBuilder channelBuilder = org.apache.kafka.clients.ClientUtils.createChannelBuilder(abstractConfig.values());
			List<String> brokerUrls = abstractConfig.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
			List<InetSocketAddress> brokerAddresses = org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses(brokerUrls);
			Cluster bootstrapCluster = Cluster.bootstrap(brokerAddresses);
			metadata.update(bootstrapCluster, 0);
			
			Long DefaultConnectionMaxIdleMs = 9 * 60 * 1000L;
			int DefaultRequestTimeoutMs = 5000;
			int DefaultMaxInFlightRequestsPerConnection = 100;
			Long DefaultReconnectBackoffMs = 50L;
			int DefaultSendBufferBytes = 128 * 1024;
			int DefaultReceiveBufferBytes = 32 * 1024;
			Long DefaultRetryBackoffMs = 100L;
			String metricGrpPrefix = "admin";
			Map<String, String> metricTags = new LinkedHashMap<String, String>();
			Selector selector = new Selector(DefaultConnectionMaxIdleMs, metrics, time, metricGrpPrefix, metricTags, channelBuilder);
			//Selector selector = new Selector(DefaultConnectionMaxIdleMs, metrics, time, metricGrpPrefix, channelBuilder);	
			AtomicInteger AdminClientIdSequence = new AtomicInteger(1);
			NetworkClient client = new NetworkClient(selector, 
					metadata, 
					"admin-" + AdminClientIdSequence.getAndIncrement(), 
					DefaultMaxInFlightRequestsPerConnection, 
					DefaultReconnectBackoffMs, 
					DefaultSendBufferBytes, 
					DefaultReceiveBufferBytes, 
					DefaultReceiveBufferBytes, 
					time);
			ConsumerNetworkClient highLevelClient = new ConsumerNetworkClient(client, metadata, time, DefaultRetryBackoffMs);
			//ConsumerNetworkClient highLevelClient = new ConsumerNetworkClient(client, metadata, time, DefaultRetryBackoffMs, DefaultRequestTimeoutMs);
			scala.collection.immutable.List<Node> nList = scala.collection.JavaConverters.asScalaBufferConverter(bootstrapCluster.nodes()).asScala().toList();
			adminClient = new AdminClient(time, DefaultRequestTimeoutMs, highLevelClient, nList);
			return adminClient;
		}
	}
	
	
	/**
	 * getFetchOffset
	 * get kafka partition fetch offset of consumer from kafka server
	 * @param groupId, topicAndPartitionList
	 * @return HashMap<Integer, PartitionMetrics>, partitionId and PartitionMetrics map. offset include in {@link PartitionMetrics}
	 * */
	public HashMap<Integer, PartitionMetrics> getFetchOffset(String groupId, List<TopicAndPartition> topicAndPartitionList){
		HashMap<Integer, PartitionMetrics> partitionMetricsMap = new HashMap<Integer, PartitionMetrics>();
		String leaderHostString = metricsKafkaServersArray[0]; 
		BlockingChannel channel = new BlockingChannel(leaderHostString, 9092,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                5000);
		GroupCoordinatorRequest groupCoordinatorRequest = new GroupCoordinatorRequest(groupId, (short) 0, 1, metricsClientId);
		try{
			channel.connect();
			channel.send(groupCoordinatorRequest);
			GroupCoordinatorResponse groupCoordinatorResponse = GroupCoordinatorResponse.readFrom(channel.receive().payload());
			String leaderHost = groupCoordinatorResponse.coordinator().host();
			channel.disconnect();
			channel = new BlockingChannel(leaderHost, 9092,
	                BlockingChannel.UseDefaultBufferSize(),
	                BlockingChannel.UseDefaultBufferSize(),
	                5000);
			channel.connect();
			OffsetFetchRequest fetchRequest = new OffsetFetchRequest(groupId, topicAndPartitionList, (short) 1, 2, metricsClientId);
			channel.send(fetchRequest.underlying());
			OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload());
			for(TopicAndPartition tp:topicAndPartitionList){
				OffsetMetadataAndError result = fetchResponse.offsets().get(tp);
	            short offsetFetchErrorCode = result.error();
	            if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
	            	LOGGER.info("NotCoordinatorForConsumerCode:consumer group re-join, retry to get leader host");
	            	//TODO consumer group re-join, retry to get leader host
	            } else if (offsetFetchErrorCode == ErrorMapping.OffsetsLoadInProgressCode()) {
	            	LOGGER.info("OffsetsLoadInProgressCode:retry the offset fetch (after backoff)");
	                //TODO retry the offset fetch (after backoff)
	            } else {
	                long retrievedOffset = result.offset();
	                PartitionMetrics pm = new PartitionMetrics(tp.partition());
	                pm.setOffset(retrievedOffset);
	                partitionMetricsMap.put(tp.partition(), pm);
	            }
			}    
		}catch (Exception e) {
			LOGGER.error("getFetchOffset", e);
		}finally{
			channel.disconnect();
		}
		return partitionMetricsMap;
	}
	
	/**
	 * getLogSize
	 * get kafka partition logSize
	 * @param groupId, sameLeaderBrokerIPTopicAndPartitionList, leaderBrokerIP
	 * @return HashMap<Integer, PartitionMetrics>, partitionId and PartitionMetrics map. logSize, leaderHost include in {@link PartitionMetrics}
	 * */
	public HashMap<Integer, PartitionMetrics> getLogSize(String groupId, List<TopicAndPartition> sameLeaderBrokerIPTopicAndPartitionList,String leaderBrokerIP){
		PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		for(TopicAndPartition tp:sameLeaderBrokerIPTopicAndPartitionList){
			requestInfo.put(tp, partitionOffsetRequestInfo);
		}
		OffsetResponse response = null;
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), metricsClientId);
		
		try{
        	SimpleConsumer dynamicConsumer = getDynamicConsumer(leaderBrokerIP);
        	synchronized (dynamicConsumer) {
        		response = dynamicConsumer.getOffsetsBefore(request);
			}
        }catch (Exception e) {
        	LOGGER.error("get logSize error", e);
        }
		if (response.hasError()){
        	LOGGER.error("OffsetResponse hase error");
        }
		
		HashMap<Integer, PartitionMetrics> partitionMetricsMap = new HashMap<Integer, PartitionMetrics>();
		
		for(TopicAndPartition tp:sameLeaderBrokerIPTopicAndPartitionList){
			String topic = tp.topic();
			int partitionId = tp.partition();
			long[] offsets = response.offsets(topic, partitionId);
			if(offsets.length > 0){
				PartitionMetrics pm = new PartitionMetrics(partitionId);
				pm.leaderHost = leaderBrokerIP;
				pm.setLogSize(offsets[0]);
				partitionMetricsMap.put(partitionId,pm);				
			}
		}
		return partitionMetricsMap;
	}
	
	
	/**
	 * getGroupIdListFromKafka
	 * get groupId list from kafka server
	 * @return List<String>, consumer groupid list
	 * */
	public List<String> getGroupIdListFromKafka(){
		List<String> consumerGroupIdList = new ArrayList<String>();
		scala.collection.immutable.Map<Node, scala.collection.immutable.List<GroupOverview>> consumerGroupMap = getAdminClient().listAllConsumerGroups();
		Map<Node, scala.collection.immutable.List<GroupOverview>> javaConsumerGroupMap = JavaConversions.asJavaMap(consumerGroupMap);
		for(Map.Entry<Node, scala.collection.immutable.List<GroupOverview> > entry: javaConsumerGroupMap.entrySet()){
			List<GroupOverview> glist = JavaConversions.asJavaList(entry.getValue());
			if(!glist.isEmpty()){
				for(GroupOverview go:glist){
					consumerGroupIdList.add(go.groupId());
				}
			}
		}
		return consumerGroupIdList;
	}
	
	/**
	 * getTopicsListFromKafka
	 * getTopicList by groupId
	 * @param groupId
	 * @return List<String>, topic list
	 * */
	public List<String> getTopicsListFromKafka(String groupId){
		scala.collection.immutable.List<ConsumerSummary> consumerSummaryList = getAdminClient().describeConsumerGroup(groupId);
		List<ConsumerSummary> consumerSummary = JavaConversions.asJavaList(consumerSummaryList);
		TreeSet<String> topicSet = new TreeSet<String>();
		for(ConsumerSummary cs:consumerSummary){
			List<TopicPartition> topicPartitionList = JavaConversions.asJavaList(cs.assignment());
			for(TopicPartition tp:topicPartitionList){
				topicSet.add(tp.topic());
			}
		}
		List<String> returnList = new ArrayList<String>(topicSet);
		return returnList;
	}
	
	/*
	 * Get GroupId and topics relationship map from kafka server
	 * */
	
	/**
	 * getGroupIdAndTopicsMapFromKafka
	 * Get groupId and topics relationship map from kafka server
	 * @return HashMap<String, List<String>>, groupId and topic list map
	 * */
	public HashMap<String, List<String>> getGroupIdAndTopicsMapFromKafka(){
		List<String> consumerGroupIdList = getGroupIdListFromKafka();
		HashMap<String, List<String>> groupAndTopicsMap = new HashMap<String, List<String>>();
		for(String groupId:consumerGroupIdList){
			List<String> topicsList = getTopicsListFromKafka(groupId);
			groupAndTopicsMap.put(groupId, topicsList);
		}
		return groupAndTopicsMap;
	}
	

	/**
	 * getPartitionInfoMapFromKafka
	 * @param groupId, topic
	 * @return HashMap<Integer, String>, partitionId and partition leaderHost map.
	 * */
	public HashMap<Integer, String> getPartitionInfoMapFromKafka(String groupId, String topic){
		KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer(groupId);
		synchronized (kafkaConsumer) {
			List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
			//kafkaConsumer.close();
			HashMap<Integer, String> partitionMap = new HashMap<Integer, String>();
			for(PartitionInfo p:partitionInfoList){
				int partitionId = p.partition();
				String host = p.leader().host();
				partitionMap.put(partitionId, host);
			}
			return partitionMap;
		}
	}
	
	
	/**
	 * getconsumerGroupMetricsSchema
	 * load topics and partitions relationship of the assigned groupId. 
	 * @param groupId
	 * @return ConsumerGroupMetrics, include partition instance owner in it.
	 * */
	public ConsumerGroupMetrics getconsumerGroupMetricsSchema(String groupId){
		ConsumerGroupMetrics cgm = new ConsumerGroupMetrics();
		cgm.groupId = groupId;
		scala.collection.immutable.List<ConsumerSummary> consumerSummaries = getAdminClient().describeConsumerGroup(groupId);
		List<ConsumerSummary> consumerSummarieslist = JavaConversions.asJavaList(consumerSummaries);
		for(int i=0; i<consumerSummarieslist.size(); i++){
			ConsumerSummary cs = consumerSummarieslist.get(i);
			String memberId = cs.memberId();
			String clientHost = cs.clientHost();
			scala.collection.immutable.List<TopicPartition> tpList = cs.assignment();
			List<TopicPartition> topicPartitionList = JavaConversions.asJavaList(tpList);
			for(TopicPartition tp:topicPartitionList){
				String topic = tp.topic();
				int partitionId = tp.partition();
				String partitionInstanceOwner = memberId+":"+clientHost;
				Map<String, String> partitionValueMap = new HashMap<String, String>();
				partitionValueMap.put("INSTANCEOWNER", partitionInstanceOwner);
				UtilHelper.fillConsumergroupMetrics(cgm, topic, partitionId, partitionValueMap);
			}
		}
		return cgm;
	}
	
	
	/**
	 * close
	 * close all the client instances in KafkaAdminConsumerClient, close socket.
	 * */
	public void close(){
		if(null != simpleConsumer){
			simpleConsumer.close();
		}
		
		if(null != adminClient){
			adminClient.close();
		}
		if(!kafkaConsumerMap.isEmpty()){
			for(Map.Entry<String, KafkaConsumer<String, String>> entry:kafkaConsumerMap.entrySet()){
				entry.getValue().close();
			}
		}
		
		if(!dynamicSimpleConsumerMap.isEmpty()){
			for(Map.Entry<String, SimpleConsumer> entry:dynamicSimpleConsumerMap.entrySet()){
				entry.getValue().close();
			}
		}
		
		if(null != zk){
			try{
				zk.close();
			}catch (Exception e) {
				LOGGER.error("close zookeeper failed", e);
			}
		}
		
	}
	
	public String[] getMetricsKafkaServersArray() {
		return metricsKafkaServersArray;
	}

	public void setMetricsKafkaServersArray(String[] metricsKafkaServersArray) {
		this.metricsKafkaServersArray = metricsKafkaServersArray;
	}


	public String[] getMetricsZookeeperServersArray() {
		return metricsZookeeperServersArray;
	}

	public void setMetricsZookeeperServersArray(String[] metricsZookeeperServersArray) {
		this.metricsZookeeperServersArray = metricsZookeeperServersArray;
	}

	public int getMetricsKafkaPort() {
		return metricsKafkaPort;
	}

	public void setMetricsKafkaPort(int metricsKafkaPort) {
		this.metricsKafkaPort = metricsKafkaPort;
	}

	public int getMetricsZookeeperPort() {
		return metricsZookeeperPort;
	}

	public void setMetricsZookeeperPort(int metricsZookeeperPort) {
		this.metricsZookeeperPort = metricsZookeeperPort;
	}
	
}

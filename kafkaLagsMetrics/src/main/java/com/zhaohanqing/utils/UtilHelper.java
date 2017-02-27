package com.zhaohanqing.utils;

import java.util.Map;

import com.zhaohanqing.model.ConsumerGroupMetrics;
import com.zhaohanqing.model.PartitionMetrics;
import com.zhaohanqing.model.TopicMetrics;

import kafka.log.Log;

public class UtilHelper {
	public static String generatekafkaBootStrapUrls(String[] kafkaServerUrls, String port){
		StringBuffer sb = new StringBuffer();
		for(String s:kafkaServerUrls){
			sb.append(s);
			sb.append(":");
			sb.append(port);
			sb.append(",");
		}
		sb.substring(0, sb.length() -1);
		return sb.toString();
	}
	
	public static void fillConsumergroupMetrics(ConsumerGroupMetrics cgm, String topic, int partitionId, Map<String, String> partitionValueMap){
		if(cgm.topicMetricsMap.containsKey(topic)){
			TopicMetrics topicMetrics = cgm.topicMetricsMap.get(topic);
			if(topicMetrics.paritionMetricsMap.containsKey(partitionId)){
				PartitionMetrics partitionMetrics = topicMetrics.paritionMetricsMap.get(partitionId);
				mergePartitionMetrics(partitionMetrics, partitionValueMap);
			}else{
				PartitionMetrics partitionMetrics = new PartitionMetrics(partitionId);
				mergePartitionMetrics(partitionMetrics, partitionValueMap);
				topicMetrics.paritionMetricsMap.put(partitionId, partitionMetrics);
			}
		}else{
			TopicMetrics topicMetrics = new TopicMetrics();
			topicMetrics.topic = topic;
			PartitionMetrics partitionMetrics = new PartitionMetrics(partitionId);
			mergePartitionMetrics(partitionMetrics, partitionValueMap);
			topicMetrics.paritionMetricsMap.put(partitionId, partitionMetrics);
			cgm.topicMetricsMap.put(topic, topicMetrics);
		}
	}
	
	private static void mergePartitionMetrics(PartitionMetrics partitionMetrics, Map<String, String> partitionValueMap){
		for(Map.Entry<String, String> entry:partitionValueMap.entrySet()){
			String partitionValueType = entry.getKey();
			String partitionValue = entry.getValue();
			if(partitionValueType.equals("OFFSET")){
				partitionMetrics.setOffset(Long.parseLong(partitionValue));
			}
			if(partitionValueType.equals("LOGSIZE")){
				partitionMetrics.setLogSize(Long.parseLong(partitionValue)); 
			}
			if(partitionValueType.equals("LEADERHOST")){
				partitionMetrics.leaderHost = partitionValue;
			}
			if(partitionValueType.equals("INSTANCEOWNER")){
				partitionMetrics.instanceOwner = partitionValue;
			}
		}
	}
}

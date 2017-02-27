package com.zhaohanqing.model;

import java.io.Serializable;
import java.util.HashMap;

public class ConsumerGroupMetrics implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 100040966984983798L;
	public String groupId;
	public HashMap<String, TopicMetrics> topicMetricsMap = new HashMap<String, TopicMetrics>();
}

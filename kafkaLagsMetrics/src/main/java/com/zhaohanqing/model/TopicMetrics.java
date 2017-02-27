package com.zhaohanqing.model;

import java.io.Serializable;
import java.util.HashMap;

public class TopicMetrics implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 8079727986259928574L;
	public String topic;
	public HashMap<Integer, PartitionMetrics> paritionMetricsMap = new HashMap<Integer, PartitionMetrics>();
}

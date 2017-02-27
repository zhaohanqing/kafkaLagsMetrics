package com.zhaohanqing.model;

import java.io.Serializable;
import java.util.HashMap;

public class ClusterMetrics implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1780127742293097379L;
	public String clusterName;
	public HashMap<String, ConsumerGroupMetrics> ConsumerGroupMetricsMap = new HashMap<String, ConsumerGroupMetrics>();
}

package com.zhaohanqing.model;

import java.io.Serializable;

public class PartitionMetrics implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1179331192204165125L;
	public PartitionMetrics(int partitionId){
		this.partitionId = partitionId;
		this.logSize = 0L;
		this.offset = 0L;
		this.lags = 0L;
	}
	private int partitionId;
	private long offset;
	private long logSize;
	private long lags;
	public String leaderHost;
	public String instanceOwner;
	
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
		if(this.logSize != 0L){
			this.lags = this.logSize - this.offset;
		}
		
	}
	public long getLogSize() {
		return logSize;
	}
	
	public void setLogSize(long logSize) {
		this.logSize = logSize;
		if(this.offset != 0L){
			this.lags = this.logSize - this.offset;
		}
	}
	public long getLags() {
		return lags;
	}
	
}

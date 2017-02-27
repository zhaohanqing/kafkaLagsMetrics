package com.zhaohanqing.model;

public class KafkaCluster {
	private String kafkaBootstrapServerURL;
	private String port;
	private String version;
	private String clusterName;
	private String description;
	public String getKafkaBootstrapServerURL() {
		return kafkaBootstrapServerURL;
	}
	public void setKafkaBootstrapServerURL(String kafkaBootstrapServerURL) {
		this.kafkaBootstrapServerURL = kafkaBootstrapServerURL;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getClusterName() {
		return clusterName;
	}
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	
	
}

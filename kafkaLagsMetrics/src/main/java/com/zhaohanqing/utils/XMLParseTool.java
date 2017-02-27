package com.zhaohanqing.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.KafkaClient;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import com.zhaohanqing.model.KafkaCluster;

public class XMLParseTool {
	
	public static List<KafkaCluster> parseKafkaCLuster(String filePath){
		ArrayList<KafkaCluster> kafkaClusterList = new ArrayList<KafkaCluster>();
		SAXReader reader = new SAXReader();
		Element root = null;
		try {
			Document document = reader.read(filePath);
			root = document.getRootElement();
			if (root == null) {
				return null;
			}
			List<Element> childElements = root.elements();
			for (Element child : childElements){
				List<Element> cluster = child.elements();
				KafkaCluster kafkaCluster = new KafkaCluster();
				for(Element clusterattribute:cluster){
					String attributeName = clusterattribute.getName();
					String attributeValue = clusterattribute.getStringValue();
					if(attributeName.equals("kafkaServerURL")){
						kafkaCluster.setKafkaBootstrapServerURL(attributeValue);
					}
					if(attributeName.equals("port")){
						kafkaCluster.setPort(attributeValue);
					}
					if(attributeName.equals("clusterName")){
						kafkaCluster.setClusterName(attributeValue);
					}
					if(attributeName.equals("version")){
						kafkaCluster.setVersion(attributeValue);
					}
					if(attributeName.equals("description")){
						kafkaCluster.setDescription(attributeValue);
					}
				}
				kafkaClusterList.add(kafkaCluster);
			}
		}catch (Exception e) {
			System.out.println(e);
		}
		return kafkaClusterList;
	}
}

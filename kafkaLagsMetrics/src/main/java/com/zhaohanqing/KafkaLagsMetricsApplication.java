package com.zhaohanqing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.zhaohanqing.model.ClusterMetrics;
import com.zhaohanqing.model.KafkaCluster;
import com.zhaohanqing.service.MetricsCollectorRunner;
import com.zhaohanqing.utils.XMLParseTool;


@SpringBootApplication
@RestController
public class KafkaLagsMetricsApplication {
	
	public KafkaLagsMetricsApplication(){
		metricsCollectorRunnerMap = Maps.newHashMap();
		metricsCollectorcallableResultList = Lists.newArrayList();
		clusterMetricsMap = Maps.newHashMap();
		isStart = false;
	}
	
	@Value("${kafka.cluster.configuration.file.path}")
	private String kafkaClusterConfigFile;
	
	@Value("${metrics.thread.pool.size}")
	private String logSizeThreadPoolSize;
	
	@Value("${metrics.collect.internal.minute}")
	private String internalMinute;
	
	private static Map<String, MetricsCollectorRunner> metricsCollectorRunnerMap;
	private static ListeningExecutorService metricsExecutorService;
	private static List<Future<ClusterMetrics>> metricsCollectorcallableResultList;
	
	private static HashMap<String, ClusterMetrics> clusterMetricsMap;
	private static boolean isStart;
	private static long startTimeMillis;
	
	private String OKOKOK = "OKOKOK";
	private String NONONO = "NONONO";
	
	@RequestMapping("/healthcheck")
	@ResponseBody
	public String HelloWorld(){
		return "OKOKOK";
	}


	@RequestMapping("/kafkametricsinit")
	@ResponseBody
	public String startKafkaMetricsCollector(){
		if(kafkaClusterConfigFile.isEmpty()){
			return NONONO;
		}
		try{
			List<KafkaCluster> kafkaClusterList = XMLParseTool.parseKafkaCLuster(kafkaClusterConfigFile);
			if(kafkaClusterList.isEmpty()){
				return NONONO;
			}
			startMetricsCollector(kafkaClusterList);
			return OKOKOK;
		}catch (Exception e) {
			return NONONO;
		}
	}
	
	
	@RequestMapping("/getMetrics")
	@ResponseBody
	public Object getMetrics(){
		try{
			clusterMetricsMap.clear();
			for(int i=0; i<metricsCollectorcallableResultList.size();i++){
				Future<ClusterMetrics> f = metricsCollectorcallableResultList.get(i);
				try{
					ClusterMetrics clusterMetrics = f.get();
					String clusterName = clusterMetrics.clusterName;
					clusterMetricsMap.put(clusterName, clusterMetrics);
				}catch (Exception e) {
					System.out.println(e);
				}	
			}
			return clusterMetricsMap;
		}catch (Exception e) {
			return NONONO;
		}
		
	}

	
	private String startMetricsCollector(List<KafkaCluster> kafkaClusterList){
		try{
			if(isStart == true){
				long now = System.currentTimeMillis();
				long internaltime = now - startTimeMillis;
				if(internaltime < Integer.parseInt(internalMinute)*60*1000){
					return OKOKOK;
				}
			}else{
				isStart = true;
			}
			startTimeMillis = System.currentTimeMillis();
			metricsExecutorService = MoreExecutors
					.listeningDecorator(Executors.newFixedThreadPool(Integer.valueOf(logSizeThreadPoolSize)));
			for(KafkaCluster cluster:kafkaClusterList){
				if(metricsCollectorRunnerMap.containsKey(cluster.getClusterName())){
					continue;
				}else{
					MetricsCollectorRunner collector = new MetricsCollectorRunner(cluster);
					metricsCollectorRunnerMap.put(cluster.getClusterName(), collector);
				}
			}
			metricsCollectorcallableResultList = metricsExecutorService.invokeAll(metricsCollectorRunnerMap.values());
			return OKOKOK;
		}catch (Exception e) {
			System.out.println(e.toString());
			return NONONO;
		}
	}
	
	public static void main(String[] args) {
		SpringApplication.run(KafkaLagsMetricsApplication.class, args);
	}
}

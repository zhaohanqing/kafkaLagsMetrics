# kafkaLagsMetrics
A tool for get kafka consumer data, it support the following:
- consumer group
- topic
- parition
- partition logsize
- partition offset
- partition lags
- partition instance owner

Requirenments:
------------------
1. Java 8+
2. Maven 3+
3. kafka 0.9.0.*
4. service port: 8080

Configuration:
------------------
application.properties example
```
kafka.cluster.configuration.file.path=KafkaCluster.xml
metrics.thread.pool.size=3
metrics.collect.internal.minute=2
```

kafkaCluster.xml example
```
?xml version="1.0" encoding="UTF-8"?>
<clusters>
	<!-- support multi-cluster metrics -->
	<!-- version support 0.9.x.x -->
	<!-- clusterName for in each cluster should not same -->
	<cluster>
		<kafkaServerURL>xxx.xxx.xxx.xxx</kafkaServerURL>
		<port>9092</port>
		<version>0.9</version>
		<clusterName>kafka cluster1</clusterName>
		<description>test1</description>
	</cluster>
	<cluster>
		<kafkaServerURL>xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx</kafkaServerURL>
		<port>9092</port>
		<version>0.9</version>
		<clusterName>kafka cluster2</clusterName>
		<description>test2</description>
	</cluster>
</clusters>
```

Building
------------------
```
cd /{projectpath}
mvn clean
mvn compile
mvn package -Dmaven.test.skip=ture
```

Running
-----------------
```
cd /{projectpath}/target
java -jar kafkaLagsMetrics-0.0.1-SNAPSHOT.jar
```
- health check: http://127.0.0.1:8080/healthcheck
- init metrics: http://127.0.0.1:8080/kafkametricsinit
- get  metrics: http://127.0.0.1:8080/getMetrics




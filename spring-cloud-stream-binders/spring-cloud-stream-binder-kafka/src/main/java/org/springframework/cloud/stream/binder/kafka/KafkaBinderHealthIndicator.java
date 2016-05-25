/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils$;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfigurationProperties;
import org.springframework.integration.kafka.core.BrokerAddress;
import org.springframework.integration.kafka.core.Partition;

/**
 * Health indicator for Kafka.
 *
 * @author Ilayaperumal Gopinathan
 */
public class KafkaBinderHealthIndicator implements HealthIndicator {

	private final KafkaMessageChannelBinder binder;

	private final KafkaBinderConfigurationProperties configurationProperties;

	public KafkaBinderHealthIndicator(KafkaMessageChannelBinder binder,
			KafkaBinderConfigurationProperties configurationProperties) {
		this.binder = binder;
		this.configurationProperties = configurationProperties;
	}

	@Override
	public Health health() {
		ZkClient zkClient = null;
		try {
			zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
					configurationProperties.getZkSessionTimeout(),
					configurationProperties.getZkConnectionTimeout(), ZKStringSerializer$.MODULE$);
			Set<String> brokersInClusterSet = new HashSet<>();
			Seq<Broker> allBrokersInCluster = ZkUtils$.MODULE$.getAllBrokersInCluster(zkClient);
			Collection<Broker> brokersInCluster = JavaConversions.asJavaCollection(allBrokersInCluster);
			for (Broker broker : brokersInCluster) {
				brokersInClusterSet.add(broker.connectionString());
			}
			Set<String> downMessages = new HashSet<>();
			for (Map.Entry<String, Collection<Partition>> entry : binder.getTopicsInUse().entrySet()) {
				for (Partition partition : entry.getValue()) {
					BrokerAddress address = binder.getConnectionFactory().getLeader(partition);
					if (!brokersInClusterSet.contains(address.toString())) {
						downMessages.add(address.toString());
					}
				}
			}
			if (downMessages.isEmpty()) {
				return Health.up().build();
			}
			return Health.down().withDetail("Following brokers are down: ", downMessages.toString()).build();
		}
		catch (Exception e) {
			return Health.down(e).build();
		}
		finally {
			if (zkClient != null) {
				try {
					zkClient.close();
				}
				catch (Exception e) {
					// ignore
				}
			}
		}
	}
}

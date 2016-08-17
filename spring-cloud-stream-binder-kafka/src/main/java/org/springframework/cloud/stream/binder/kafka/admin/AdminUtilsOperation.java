/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.admin;

import java.util.Properties;

import kafka.utils.ZkUtils;

/**
 * API around {@link kafka.admin.AdminUtils} to support
 * various versions of Kafka brokers.
 *
 * Note: Implementations that support Kafka brokers other than 0.9, need to use
 * a possible strategy that involves reflection around {@link kafka.admin.AdminUtils}.
 *
 * @author Soby Chacko
 */
public interface AdminUtilsOperation {

	/**
	 * Invoke {@link kafka.admin.AdminUtils#addPartitions}
	 *
	 * @param zkUtils Zookeeper utils
	 * @param topic name of the topic
	 * @param numPartitions
	 * @param replicaAssignmentStr
	 * @param checkBrokerAvailable
	 */
	void invokeAddPartitions(ZkUtils zkUtils, String topic, int numPartitions,
							String replicaAssignmentStr, boolean checkBrokerAvailable);

	/**
	 * Invoke {@link kafka.admin.AdminUtils#fetchTopicMetadataFromZk}
	 *
	 * @param topic name
	 * @param zkUtils zookeeper utils
	 * @return error code
	 */
	short errorCodeFromTopicMetadata(String topic, ZkUtils zkUtils);

	/**
	 * Find partition size from Kafka broker using {@link kafka.admin.AdminUtils}
	 *
	 * @param topic name
	 * @param zkUtils zookeeper utils
	 * @return partition size
	 */
	int partitionSize(String topic, ZkUtils zkUtils);

	/**
	 * Inovke {@link kafka.admin.AdminUtils#createTopic}
	 *
	 * @param zkUtils zookeeper utils
	 * @param topic name
	 * @param partitions
	 * @param replicationFactor
	 * @param topicConfig
	 */
	void invokeCreateTopic(ZkUtils zkUtils, String topic, int partitions,
					int replicationFactor, Properties topicConfig);
}

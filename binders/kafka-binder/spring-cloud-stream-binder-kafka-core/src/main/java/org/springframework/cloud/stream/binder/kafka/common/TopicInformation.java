/*
 * Copyright 2023-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.common;

import java.util.Collection;

import org.apache.kafka.common.PartitionInfo;

/**
 * Record to capture topic information for various binder related tasks.
 *
 * @param consumerGroup consumer group for the consumer
 * @param partitionInfos collection of {@link PartitionInfo}
 * @param isTopicPattern if the topic is specified as a pattern
 *
 * @author Soby Chacko (and previous authors before refactoring).
 */
public record TopicInformation(String consumerGroup, Collection<PartitionInfo> partitionInfos, boolean isTopicPattern) {

	public boolean isConsumerTopic() {
		return this.consumerGroup != null;
	}
}

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
 * @author Soby Chacko
 * @since 4.1.0
 */
public class TopicInformation {

	private final String consumerGroup;

	private final Collection<PartitionInfo> partitionInfos;

	private final boolean isTopicPattern;

	public TopicInformation(String consumerGroup, Collection<PartitionInfo> partitionInfos,
					boolean isTopicPattern) {
		this.consumerGroup = consumerGroup;
		this.partitionInfos = partitionInfos;
		this.isTopicPattern = isTopicPattern;
	}

	public String getConsumerGroup() {
		return this.consumerGroup;
	}

	public boolean isConsumerTopic() {
		return this.consumerGroup != null;
	}

	public boolean isTopicPattern() {
		return this.isTopicPattern;
	}

	public Collection<PartitionInfo> getPartitionInfos() {
		return this.partitionInfos;
	}
}

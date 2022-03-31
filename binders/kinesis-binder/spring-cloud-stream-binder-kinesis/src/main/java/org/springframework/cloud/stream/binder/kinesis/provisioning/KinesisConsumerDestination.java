/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kinesis.provisioning;

import java.util.List;

import com.amazonaws.services.kinesis.model.Shard;

import org.springframework.cloud.stream.provisioning.ConsumerDestination;

/**
 * The Kinesis-specific {@link ConsumerDestination} implementation.
 *
 * @author Artem Bilan
 *
 */
public final class KinesisConsumerDestination implements ConsumerDestination {

	private final String streamName;

	private final List<Shard> shards;

	private final String dlqName;

	public KinesisConsumerDestination(String streamName, List<Shard> shards) {
		this(streamName, shards, null);
	}

	public KinesisConsumerDestination(String streamName, List<Shard> shards, String dlqName) {
		this.streamName = streamName;
		this.shards = shards;
		this.dlqName = dlqName;
	}

	@Override
	public String getName() {
		return this.streamName;
	}

	public List<Shard> getShards() {
		return this.shards;
	}

	@Override
	public String toString() {
		return "KinesisConsumerDestination{" + "streamName='" + this.streamName + '\''
				+ ", shards=" + this.shards + ", dlqName='" + this.dlqName + '\'' + '}';
	}

}

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

package org.springframework.cloud.stream.binder.kinesis.properties;

import org.springframework.integration.aws.inbound.kinesis.CheckpointMode;
import org.springframework.integration.aws.inbound.kinesis.ListenerMode;

/**
 * The Kinesis-specific consumer binding configuration properties.
 *
 * @author Peter Oates
 * @author Jacob Severson
 * @author Artem Bilan
 * @author Arnaud Lecollaire
 * @author Dmytro Danilenkov
 *
 */
public class KinesisConsumerProperties {

	private int startTimeout = 60000;

	private ListenerMode listenerMode = ListenerMode.record;

	private CheckpointMode checkpointMode = CheckpointMode.batch;

	/**
	 * Interval, in milliseconds, between two checkpoints when checkpoint mode is periodic.
	 */
	private Long checkpointInterval = 5_000L;

	private int recordsLimit = 10000;

	private int idleBetweenPolls = 1000;

	private int consumerBackoff = 1000;

	private String shardIteratorType;

	private boolean dynamoDbStreams;

	private String shardId;

	/**
	 * Worker identifier used to distinguish different workers/processes
	 * (only used when KCL is enabled).
	 */
	private String workerId;

	public int getStartTimeout() {
		return this.startTimeout;
	}

	public void setStartTimeout(int startTimeout) {
		this.startTimeout = startTimeout;
	}

	public ListenerMode getListenerMode() {
		return this.listenerMode;
	}

	public void setListenerMode(ListenerMode listenerMode) {
		this.listenerMode = listenerMode;
	}

	public CheckpointMode getCheckpointMode() {
		return this.checkpointMode;
	}

	public void setCheckpointMode(CheckpointMode checkpointMode) {
		this.checkpointMode = checkpointMode;
	}

	public Long getCheckpointInterval() {
		return checkpointInterval;
	}

	public void setCheckpointInterval(Long checkpointInterval) {
		this.checkpointInterval = checkpointInterval;
	}

	public int getRecordsLimit() {
		return this.recordsLimit;
	}

	public void setRecordsLimit(int recordsLimit) {
		this.recordsLimit = recordsLimit;
	}

	public int getIdleBetweenPolls() {
		return this.idleBetweenPolls;
	}

	public void setIdleBetweenPolls(int idleBetweenPolls) {
		this.idleBetweenPolls = idleBetweenPolls;
	}

	public int getConsumerBackoff() {
		return this.consumerBackoff;
	}

	public void setConsumerBackoff(int consumerBackoff) {
		this.consumerBackoff = consumerBackoff;
	}

	public String getShardIteratorType() {
		return this.shardIteratorType;
	}

	public void setShardIteratorType(String shardIteratorType) {
		this.shardIteratorType = shardIteratorType;
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public boolean isDynamoDbStreams() {
		return this.dynamoDbStreams;
	}

	public void setDynamoDbStreams(boolean dynamoDbStreams) {
		this.dynamoDbStreams = dynamoDbStreams;
	}

	public String getShardId() {
		return shardId;
	}

	public void setShardId(String shardId) {
		this.shardId = shardId;
	}
}

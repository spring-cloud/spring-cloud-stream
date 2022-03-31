/*
 * Copyright 2017-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ScalingType;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.amazonaws.services.kinesis.model.UpdateShardCountRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisConsumerProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.util.Assert;

/**
 * The {@link ProvisioningProvider} implementation for Amazon Kinesis.
 *
 * @author Peter Oates
 * @author Artem Bilan
 * @author Jacob Severson
 * @author Sergiu Pantiru
 * @author Matthias Wesolowski
 */
public class KinesisStreamProvisioner implements
		ProvisioningProvider<ExtendedConsumerProperties<KinesisConsumerProperties>,
				ExtendedProducerProperties<KinesisProducerProperties>> {

	private static final Log logger = LogFactory.getLog(KinesisStreamProvisioner.class);

	private final AmazonKinesis amazonKinesis;

	private final KinesisBinderConfigurationProperties configurationProperties;

	public KinesisStreamProvisioner(AmazonKinesis amazonKinesis,
			KinesisBinderConfigurationProperties kinesisBinderConfigurationProperties) {

		Assert.notNull(amazonKinesis, "'amazonKinesis' must not be null");
		Assert.notNull(kinesisBinderConfigurationProperties,
				"'kinesisBinderConfigurationProperties' must not be null");
		this.amazonKinesis = amazonKinesis;
		this.configurationProperties = kinesisBinderConfigurationProperties;
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name,
			ExtendedProducerProperties<KinesisProducerProperties> properties)
			throws ProvisioningException {

		if (logger.isInfoEnabled()) {
			logger.info("Using Kinesis stream for outbound: " + name);
		}

		if (properties.getHeaderMode() == null) {
			properties.setHeaderMode(HeaderMode.embeddedHeaders);
		}

		return new KinesisProducerDestination(name,
				createOrUpdate(name, properties.getPartitionCount()));
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
			ExtendedConsumerProperties<KinesisConsumerProperties> properties)
			throws ProvisioningException {

		if (properties.getExtension().isDynamoDbStreams()) {
			if (logger.isInfoEnabled()) {
				logger.info("Using DynamoDB table in DynamoDB Streams support for inbound: " + name);
			}
			return new KinesisConsumerDestination(name, Collections.emptyList());
		}

		if (logger.isInfoEnabled()) {
			logger.info("Using Kinesis stream for inbound: " + name);
		}

		if (properties.getHeaderMode() == null) {
			properties.setHeaderMode(HeaderMode.embeddedHeaders);
		}

		int shardCount = properties.getInstanceCount() * properties.getConcurrency();

		return new KinesisConsumerDestination(name, createOrUpdate(name, shardCount));
	}

	private List<Shard> getShardList(String stream) {
		return this.getShardList(stream, 0);
	}

	private List<Shard> getShardList(String stream, int retryCount) {
		List<Shard> shardList = new ArrayList<>();

		if (retryCount++ > configurationProperties.getDescribeStreamRetries()) {
			ResourceNotFoundException resourceNotFoundException = new ResourceNotFoundException(
					"The stream [" + stream + "] isn't ACTIVE or doesn't exist.");
			resourceNotFoundException.setServiceName("Kinesis");

			throw new ProvisioningException(
					"Kinesis org.springframework.cloud.stream.binder.kinesis.provisioning error",
					resourceNotFoundException);
		}

		ListShardsRequest listShardsRequest = new ListShardsRequest().withStreamName(stream);

		try {
			ListShardsResult listShardsResult = amazonKinesis.listShards(listShardsRequest);

			shardList.addAll(listShardsResult.getShards());

		}
		catch (LimitExceededException limitExceededException) {
			logger.info("Got LimitExceededException when describing stream [" + stream + "]. " + "Backing off for ["
					+ this.configurationProperties.getDescribeStreamBackoff() + "] millis.");

			try {
				Thread.sleep(this.configurationProperties.getDescribeStreamBackoff());
				getShardList(stream, retryCount);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new ProvisioningException(
						"The [describeStream] thread for the stream [" + stream + "] has been interrupted.", ex);
			}
		}

		return shardList;
	}

	private List<Shard> createOrUpdate(String stream, int shards) {
		List<Shard> shardList = new ArrayList<>();
		try {
			shardList = getShardList(stream);
		}
		catch (ResourceNotFoundException ex) {
			if (!this.configurationProperties.isAutoCreateStream()) {
				throw new ProvisioningException(
						"The stream [" + stream + "] was not found and auto creation is disabled.", ex);
			}
			if (logger.isInfoEnabled()) {
				logger.info("Stream '" + stream + "' not found. Create one...");
			}

			this.amazonKinesis.createStream(stream, Math.max(this.configurationProperties.getMinShardCount(), shards));

			waitForStreamToBecomeActive(stream);
		}

		int effectiveShardCount = Math.max(this.configurationProperties.getMinShardCount(), shards);

		if ((shardList.size() < effectiveShardCount)
				&& this.configurationProperties.isAutoAddShards()) {
			return updateShardCount(stream, shardList.size(), effectiveShardCount);
		}

		return shardList;
	}

	private void waitForStreamToBecomeActive(String streamName) {
		int describeStreamRetries = 0;
		while (true) {
			try {
				DescribeStreamResult describeStreamResult = this.amazonKinesis.describeStream(streamName);
				if (describeStreamResult != null &&
					StreamStatus.ACTIVE.name().equals(describeStreamResult.getStreamDescription().getStreamStatus())) {

					return;
				}
				else if (describeStreamRetries++ > this.configurationProperties.getDescribeStreamRetries()) {
						ResourceNotFoundException resourceNotFoundException = new ResourceNotFoundException(
							"The stream [" + streamName + "] isn't ACTIVE or doesn't exist.");
						resourceNotFoundException.setServiceName("Kinesis");
						throw new ProvisioningException(
							"Kinesis org.springframework.cloud.stream.binder.kinesis.provisioning error",
							resourceNotFoundException);
				}
			}
			catch (LimitExceededException ex) {
				logger.info("Got LimitExceededException when describing stream [" + streamName + "]. " +
					"Backing off for [" + this.configurationProperties.getDescribeStreamBackoff() + "] millis.");
			}

			try {
				Thread.sleep(this.configurationProperties.getDescribeStreamBackoff());
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new ProvisioningException(
					"The [describeStream] thread for the stream [" + streamName + "] has been interrupted.", ex);
			}
		}
	}

	private List<Shard> updateShardCount(String streamName, int shardCount, int targetCount) {
		if (logger.isInfoEnabled()) {
			logger.info("Stream [" + streamName + "] has [" + shardCount
					+ "] shards compared to a target configuration of [" + targetCount
					+ "], creating shards...");
		}

		UpdateShardCountRequest updateShardCountRequest = new UpdateShardCountRequest()
				.withStreamName(streamName).withTargetShardCount(targetCount)
				.withScalingType(ScalingType.UNIFORM_SCALING);

		this.amazonKinesis.updateShardCount(updateShardCountRequest);

		// Wait for stream to become active again after resharding
		List<Shard> shardList = new ArrayList<>();

		int describeStreamRetries = 0;

		String exclusiveStartShardId = null;

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest()
				.withStreamName(streamName);

		while (true) {
			DescribeStreamResult describeStreamResult = null;

			try {
				describeStreamRequest.withExclusiveStartShardId(exclusiveStartShardId);
				describeStreamResult = this.amazonKinesis.describeStream(describeStreamRequest);
				StreamDescription streamDescription = describeStreamResult.getStreamDescription();
				if (StreamStatus.ACTIVE.toString().equals(streamDescription.getStreamStatus())) {
					shardList.addAll(streamDescription.getShards());

					if (streamDescription.getHasMoreShards()) {
						exclusiveStartShardId = shardList.get(shardList.size() - 1).getShardId();
					}
					else {
						break;
					}
				}
			}
			catch (LimitExceededException ex) {
				logger.info("Got LimitExceededException when describing stream [" + streamName + "]. "
					+ "Backing off for [" + this.configurationProperties.getDescribeStreamBackoff() + "] millis.");
			}

			if (describeStreamResult == null || !StreamStatus.ACTIVE.toString().equals(
					describeStreamResult.getStreamDescription().getStreamStatus())) {
				if (describeStreamRetries++ > this.configurationProperties.getDescribeStreamRetries()) {
					ResourceNotFoundException resourceNotFoundException = new ResourceNotFoundException(
							"The stream [" + streamName + "] isn't ACTIVE or doesn't exist.");
					resourceNotFoundException.setServiceName("Kinesis");
					throw new ProvisioningException(
							"Kinesis org.springframework.cloud.stream.binder.kinesis.provisioning error",
							resourceNotFoundException);
				}
				try {
					Thread.sleep(this.configurationProperties.getDescribeStreamBackoff());
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new ProvisioningException(
							"The [describeStream] thread for the stream [" + streamName + "] has been interrupted.", ex);
				}
			}
		}
		return shardList;
	}

}

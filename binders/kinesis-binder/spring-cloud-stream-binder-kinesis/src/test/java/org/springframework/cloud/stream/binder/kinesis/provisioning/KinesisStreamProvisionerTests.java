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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ScalingType;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.amazonaws.services.kinesis.model.UpdateShardCountRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisConsumerProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link KinesisStreamProvisioner}.
 *
 * @author Jacob Severson
 * @author Artem Bilan
 * @author Sergiu Pantiru
 */
class KinesisStreamProvisionerTests {

	@Test
	void testProvisionProducerSuccessfulWithExistingStream() {
		AmazonKinesis amazonKinesisMock = mock(AmazonKinesis.class);
		KinesisBinderConfigurationProperties binderProperties = new KinesisBinderConfigurationProperties();
		KinesisStreamProvisioner provisioner = new KinesisStreamProvisioner(
				amazonKinesisMock, binderProperties);
		ExtendedProducerProperties<KinesisProducerProperties> extendedProducerProperties =
				new ExtendedProducerProperties<>(new KinesisProducerProperties());
		String name = "test-stream";

		when(amazonKinesisMock.listShards(any(ListShardsRequest.class)))
				.thenReturn(new ListShardsResult().withShards(new Shard()));
		ProducerDestination destination = provisioner.provisionProducerDestination(name,
				extendedProducerProperties);

		verify(amazonKinesisMock).listShards(any(ListShardsRequest.class));

		assertThat(destination.getName()).isEqualTo(name);
	}

	@Test
	void testProvisionConsumerSuccessfulWithExistingStream() {
		AmazonKinesis amazonKinesisMock = mock(AmazonKinesis.class);
		KinesisBinderConfigurationProperties binderProperties = new KinesisBinderConfigurationProperties();
		KinesisStreamProvisioner provisioner = new KinesisStreamProvisioner(
				amazonKinesisMock, binderProperties);

		ExtendedConsumerProperties<KinesisConsumerProperties> extendedConsumerProperties =
				new ExtendedConsumerProperties<>(
						new KinesisConsumerProperties());

		String name = "test-stream";
		String group = "test-group";

		when(amazonKinesisMock.listShards(any(ListShardsRequest.class)))
				.thenReturn(new ListShardsResult().withShards(new Shard()));

		ConsumerDestination destination = provisioner.provisionConsumerDestination(name,
				group, extendedConsumerProperties);

		verify(amazonKinesisMock).listShards(any(ListShardsRequest.class));

		assertThat(destination.getName()).isEqualTo(name);
	}

	@Test
	void testProvisionConsumerExistingStreamUpdateShards() {
		AmazonKinesis amazonKinesisMock = mock(AmazonKinesis.class);
		ArgumentCaptor<UpdateShardCountRequest> updateShardCaptor = ArgumentCaptor
				.forClass(UpdateShardCountRequest.class);
		String name = "test-stream";
		String group = "test-group";
		int targetShardCount = 2;
		KinesisBinderConfigurationProperties binderProperties = new KinesisBinderConfigurationProperties();
		binderProperties.setMinShardCount(targetShardCount);
		binderProperties.setAutoAddShards(true);
		KinesisStreamProvisioner provisioner = new KinesisStreamProvisioner(
				amazonKinesisMock, binderProperties);

		ExtendedConsumerProperties<KinesisConsumerProperties> extendedConsumerProperties =
				new ExtendedConsumerProperties<>(
						new KinesisConsumerProperties());

		DescribeStreamResult describeOriginalStream = describeStreamResultWithShards(
				Collections.singletonList(new Shard()));

		DescribeStreamResult describeUpdatedStream = describeStreamResultWithShards(
				Arrays.asList(new Shard(), new Shard()));

		when(amazonKinesisMock.describeStream(any(DescribeStreamRequest.class)))
				.thenReturn(describeOriginalStream).thenReturn(describeUpdatedStream);

		when(amazonKinesisMock.listShards(any(ListShardsRequest.class)))
				.thenReturn(new ListShardsResult().withShards(new Shard()))
				.thenReturn(new ListShardsResult().withShards(new Shard(), new Shard()));

		provisioner.provisionConsumerDestination(name, group, extendedConsumerProperties);

		verify(amazonKinesisMock, times(1)).updateShardCount(updateShardCaptor.capture());

		assertThat(updateShardCaptor.getValue().getStreamName()).isEqualTo(name);
		assertThat(updateShardCaptor.getValue().getScalingType())
				.isEqualTo(ScalingType.UNIFORM_SCALING.name());
		assertThat(updateShardCaptor.getValue().getTargetShardCount())
				.isEqualTo(targetShardCount);
	}

	@Test
	void testProvisionProducerSuccessfulWithNewStream() {
		AmazonKinesis amazonKinesisMock = mock(AmazonKinesis.class);
		KinesisBinderConfigurationProperties binderProperties = new KinesisBinderConfigurationProperties();
		KinesisStreamProvisioner provisioner = new KinesisStreamProvisioner(
				amazonKinesisMock, binderProperties);
		ExtendedProducerProperties<KinesisProducerProperties> extendedProducerProperties =
				new ExtendedProducerProperties<>(
						new KinesisProducerProperties());

		String name = "test-stream";
		Integer shards = 1;

		when(amazonKinesisMock.listShards(any(ListShardsRequest.class)))
				.thenThrow(new ResourceNotFoundException("I got nothing"))
				.thenReturn(new ListShardsResult().withShards(new Shard()));


		when(amazonKinesisMock.createStream(name, shards))
				.thenReturn(new CreateStreamResult());

		when(amazonKinesisMock.describeStream(name))
			.thenReturn(new DescribeStreamResult()
				.withStreamDescription(new StreamDescription()
					.withStreamStatus(StreamStatus.ACTIVE)));

		ProducerDestination destination = provisioner.provisionProducerDestination(name,
				extendedProducerProperties);

		verify(amazonKinesisMock)
				.listShards(any(ListShardsRequest.class));

		verify(amazonKinesisMock).createStream(name, shards);

		assertThat(destination.getName()).isEqualTo(name);
	}

	@Test
	void testProvisionProducerUpdateShards() {
		AmazonKinesis amazonKinesisMock = mock(AmazonKinesis.class);
		ArgumentCaptor<UpdateShardCountRequest> updateShardCaptor = ArgumentCaptor
				.forClass(UpdateShardCountRequest.class);
		String name = "test-stream";
		String group = "test-group";
		int targetShardCount = 2;
		KinesisBinderConfigurationProperties binderProperties = new KinesisBinderConfigurationProperties();
		binderProperties.setMinShardCount(targetShardCount);
		binderProperties.setAutoAddShards(true);
		KinesisStreamProvisioner provisioner = new KinesisStreamProvisioner(
				amazonKinesisMock, binderProperties);

		ExtendedConsumerProperties<KinesisConsumerProperties> extendedConsumerProperties =
				new ExtendedConsumerProperties<>(
						new KinesisConsumerProperties());

		DescribeStreamResult describeOriginalStream = describeStreamResultWithShards(
				Collections.singletonList(new Shard()));

		DescribeStreamResult describeUpdatedStream = describeStreamResultWithShards(
				Arrays.asList(new Shard(), new Shard()));

		when(amazonKinesisMock.describeStream(any(DescribeStreamRequest.class)))
				.thenReturn(describeOriginalStream).thenReturn(describeUpdatedStream);

		when(amazonKinesisMock.listShards(any(ListShardsRequest.class)))
				.thenReturn(new ListShardsResult().withShards(new Shard()))
				.thenReturn(new ListShardsResult().withShards(new Shard(), new Shard()));

		provisioner.provisionConsumerDestination(name, group, extendedConsumerProperties);

		verify(amazonKinesisMock, times(1)).updateShardCount(updateShardCaptor.capture());
		assertThat(updateShardCaptor.getValue().getStreamName()).isEqualTo(name);
		assertThat(updateShardCaptor.getValue().getScalingType())
				.isEqualTo(ScalingType.UNIFORM_SCALING.name());
		assertThat(updateShardCaptor.getValue().getTargetShardCount())
				.isEqualTo(targetShardCount);
	}

	@Test
	void testProvisionConsumerSuccessfulWithNewStream() {
		AmazonKinesis amazonKinesisMock = mock(AmazonKinesis.class);
		KinesisBinderConfigurationProperties binderProperties = new KinesisBinderConfigurationProperties();
		KinesisStreamProvisioner provisioner = new KinesisStreamProvisioner(
				amazonKinesisMock, binderProperties);
		int instanceCount = 1;
		int concurrency = 1;

		ExtendedConsumerProperties<KinesisConsumerProperties> extendedConsumerProperties =
				new ExtendedConsumerProperties<>(
						new KinesisConsumerProperties());
		extendedConsumerProperties.setInstanceCount(instanceCount);
		extendedConsumerProperties.setConcurrency(concurrency);

		String name = "test-stream";
		String group = "test-group";

		when(amazonKinesisMock.listShards(any(ListShardsRequest.class)))
				.thenThrow(new ResourceNotFoundException("I got nothing"))
				.thenReturn(new ListShardsResult().withShards(new Shard()));

		when(amazonKinesisMock.createStream(name, instanceCount * concurrency))
				.thenReturn(new CreateStreamResult());

		when(amazonKinesisMock.describeStream(name))
			.thenReturn(new DescribeStreamResult()
				.withStreamDescription(new StreamDescription()
					.withStreamStatus(StreamStatus.ACTIVE)));

		ConsumerDestination destination = provisioner.provisionConsumerDestination(name,
				group, extendedConsumerProperties);

		verify(amazonKinesisMock, times(1))
				.listShards(any(ListShardsRequest.class));

		verify(amazonKinesisMock).createStream(name, instanceCount * concurrency);

		assertThat(destination.getName()).isEqualTo(name);
	}

	private static DescribeStreamResult describeStreamResultWithShards(
			List<Shard> shards) {
		return new DescribeStreamResult().withStreamDescription(new StreamDescription()
				.withShards(shards).withStreamStatus(StreamStatus.ACTIVE)
				.withHasMoreShards(Boolean.FALSE));
	}

	@Test
	void testProvisionConsumerResourceNotFoundException() {
		AmazonKinesis amazonKinesisMock = mock(AmazonKinesis.class);
		KinesisBinderConfigurationProperties binderProperties = new KinesisBinderConfigurationProperties();
		binderProperties.setAutoCreateStream(false);
		KinesisStreamProvisioner provisioner = new KinesisStreamProvisioner(
				amazonKinesisMock, binderProperties);
		int instanceCount = 1;
		int concurrency = 1;

		ExtendedConsumerProperties<KinesisConsumerProperties> extendedConsumerProperties =
				new ExtendedConsumerProperties<>(
						new KinesisConsumerProperties());
		extendedConsumerProperties.setInstanceCount(instanceCount);
		extendedConsumerProperties.setConcurrency(concurrency);

		String name = "test-stream";
		String group = "test-group";

		when(amazonKinesisMock.listShards(any(ListShardsRequest.class)))
				.thenThrow(new ResourceNotFoundException("Stream not found"));

		assertThatThrownBy(() -> provisioner.provisionConsumerDestination(name, group,
				extendedConsumerProperties))
				.isInstanceOf(ProvisioningException.class)
				.hasMessageContaining(
						"The stream [test-stream] was not found and auto creation is disabled.")
				.hasCauseInstanceOf(ResourceNotFoundException.class);

		verify(amazonKinesisMock, times(1))
				.listShards(any(ListShardsRequest.class));

		verify(amazonKinesisMock, never()).createStream(name,
				instanceCount * concurrency);
	}

}

/*
 * Copyright 2017-2020 the original author or authors.
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

package org.springframework.cloud.stream.binder.kinesis.config;

import java.util.List;
import java.util.Set;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import io.awspring.cloud.autoconfigure.context.ContextCredentialsAutoConfiguration;
import io.awspring.cloud.autoconfigure.context.ContextRegionProviderAutoConfiguration;
import io.awspring.cloud.core.region.RegionProvider;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.kinesis.KinesisBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kinesis.KinesisMessageChannelBinder;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kinesis.provisioning.KinesisStreamProvisioner;
import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.cloud.stream.config.ConsumerEndpointCustomizer;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.aws.lock.DynamoDbLockRegistry;
import org.springframework.integration.aws.metadata.DynamoDbMetadataStore;
import org.springframework.integration.aws.outbound.AbstractAwsMessageHandler;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.support.locks.LockRegistry;

/**
 * The auto-configuration for AWS components and Spring Cloud Stream Kinesis Binder.
 *
 * @author Peter Oates
 * @author Artem Bilan
 * @author Arnaud Lecollaire
 * @author Asiel Caballero
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnMissingBean(Binder.class)
@EnableConfigurationProperties({ KinesisBinderConfigurationProperties.class, KinesisExtendedBindingProperties.class })
@Import({ ContextCredentialsAutoConfiguration.class, ContextRegionProviderAutoConfiguration.class })
public class KinesisBinderConfiguration {

	private final KinesisBinderConfigurationProperties configurationProperties;

	private final AWSCredentialsProvider awsCredentialsProvider;

	private final String region;

	private final boolean hasInputs;

	public KinesisBinderConfiguration(KinesisBinderConfigurationProperties configurationProperties,
			AWSCredentialsProvider awsCredentialsProvider,
			RegionProvider regionProvider,
			List<Bindable> bindables) {

		this.configurationProperties = configurationProperties;
		this.awsCredentialsProvider = awsCredentialsProvider;
		this.region = regionProvider.getRegion().getName();
		this.hasInputs =
				bindables.stream()
						.map(Bindable::getInputs)
						.flatMap(Set::stream)
						.findFirst()
						.isPresent();
	}

	@Bean
	@ConditionalOnMissingBean
	public AmazonKinesisAsync amazonKinesis() {
		return AmazonKinesisAsyncClientBuilder.standard()
				.withCredentials(this.awsCredentialsProvider)
				.withRegion(this.region)
				.build();
	}

	@Bean
	public KinesisStreamProvisioner provisioningProvider(AmazonKinesisAsync amazonKinesis) {
		return new KinesisStreamProvisioner(amazonKinesis, this.configurationProperties);
	}

	@Bean
	@ConditionalOnMissingBean
	public AmazonDynamoDBAsync dynamoDB() {
		if (this.hasInputs) {
			return AmazonDynamoDBAsyncClientBuilder.standard()
					.withCredentials(this.awsCredentialsProvider)
					.withRegion(this.region)
					.build();
		}
		else {
			return null;
		}
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnBean(AmazonDynamoDBAsync.class)
	@ConditionalOnProperty(name = "spring.cloud.stream.kinesis.binder.kpl-kcl-enabled", havingValue = "false",
			matchIfMissing = true)
	public LockRegistry dynamoDBLockRegistry(@Autowired(required = false) AmazonDynamoDBAsync dynamoDB) {
		if (dynamoDB != null) {
			KinesisBinderConfigurationProperties.Locks locks = this.configurationProperties.getLocks();
			DynamoDbLockRegistry dynamoDbLockRegistry = new DynamoDbLockRegistry(dynamoDB, locks.getTable());
			dynamoDbLockRegistry.setRefreshPeriod(locks.getRefreshPeriod());
			dynamoDbLockRegistry.setHeartbeatPeriod(locks.getHeartbeatPeriod());
			dynamoDbLockRegistry.setLeaseDuration(locks.getLeaseDuration());
			dynamoDbLockRegistry.setPartitionKey(locks.getPartitionKey());
			dynamoDbLockRegistry.setSortKeyName(locks.getSortKeyName());
			dynamoDbLockRegistry.setSortKey(locks.getSortKey());
			dynamoDbLockRegistry.setBillingMode(locks.getBillingMode());
			dynamoDbLockRegistry.setReadCapacity(locks.getReadCapacity());
			dynamoDbLockRegistry.setWriteCapacity(locks.getWriteCapacity());
			return dynamoDbLockRegistry;
		}
		else {
			return null;
		}
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnBean(AmazonDynamoDBAsync.class)
	@ConditionalOnProperty(name = "spring.cloud.stream.kinesis.binder.kpl-kcl-enabled", havingValue = "false",
			matchIfMissing = true)
	public ConcurrentMetadataStore kinesisCheckpointStore(@Autowired(required = false) AmazonDynamoDBAsync dynamoDB) {
		if (dynamoDB != null) {
			KinesisBinderConfigurationProperties.Checkpoint checkpoint = this.configurationProperties.getCheckpoint();
			DynamoDbMetadataStore kinesisCheckpointStore = new DynamoDbMetadataStore(dynamoDB, checkpoint.getTable());
			kinesisCheckpointStore.setBillingMode(checkpoint.getBillingMode());
			kinesisCheckpointStore.setReadCapacity(checkpoint.getReadCapacity());
			kinesisCheckpointStore.setWriteCapacity(checkpoint.getWriteCapacity());
			kinesisCheckpointStore.setCreateTableDelay(checkpoint.getCreateDelay());
			kinesisCheckpointStore.setCreateTableRetries(checkpoint.getCreateRetries());
			if (checkpoint.getTimeToLive() != null) {
				kinesisCheckpointStore.setTimeToLive(checkpoint.getTimeToLive());
			}
			return kinesisCheckpointStore;
		}
		else {
			return null;
		}
	}

	@Bean
	@ConditionalOnMissingBean
	public AmazonDynamoDBStreams dynamoDBStreams() {
		if (this.hasInputs) {
			return AmazonDynamoDBStreamsClientBuilder.standard()
					.withCredentials(this.awsCredentialsProvider)
					.withRegion(this.region)
					.build();
		}
		else {
			return null;
		}
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(name = "spring.cloud.stream.kinesis.binder.kpl-kcl-enabled")
	public AmazonCloudWatchAsync cloudWatch() {
		if (this.hasInputs) {
			return AmazonCloudWatchAsyncClientBuilder.standard()
					.withCredentials(this.awsCredentialsProvider)
					.withRegion(this.region)
					.build();
		}
		else {
			return null;
		}
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(name = "spring.cloud.stream.kinesis.binder.kpl-kcl-enabled")
	public KinesisProducerConfiguration kinesisProducerConfiguration() {
		KinesisProducerConfiguration kinesisProducerConfiguration = new KinesisProducerConfiguration();
		kinesisProducerConfiguration.setCredentialsProvider(this.awsCredentialsProvider);
		kinesisProducerConfiguration.setRegion(this.region);
		return kinesisProducerConfiguration;
	}

	@Bean
	public KinesisMessageChannelBinder kinesisMessageChannelBinder(
			KinesisStreamProvisioner provisioningProvider,
			AmazonKinesisAsync amazonKinesis,
			KinesisExtendedBindingProperties kinesisExtendedBindingProperties,
			@Autowired(required = false) ConcurrentMetadataStore kinesisCheckpointStore,
			@Autowired(required = false) LockRegistry lockRegistry,
			@Autowired(required = false) AmazonDynamoDB dynamoDBClient,
			@Autowired(required = false) AmazonDynamoDBStreams dynamoDBStreams,
			@Autowired(required = false) AmazonCloudWatch cloudWatchClient,
			@Autowired(required = false) KinesisProducerConfiguration kinesisProducerConfiguration,
			@Autowired(required = false) ProducerMessageHandlerCustomizer<? extends AbstractAwsMessageHandler<Void>> producerMessageHandlerCustomizer,
			@Autowired(required = false) ConsumerEndpointCustomizer<? extends MessageProducerSupport> consumerEndpointCustomizer,
			@Autowired List<KinesisClientLibConfiguration> kinesisClientLibConfigurations) {

		KinesisMessageChannelBinder kinesisMessageChannelBinder =
				new KinesisMessageChannelBinder(this.configurationProperties, provisioningProvider, amazonKinesis,
						this.awsCredentialsProvider, dynamoDBClient, dynamoDBStreams, cloudWatchClient);
		kinesisMessageChannelBinder.setCheckpointStore(kinesisCheckpointStore);
		kinesisMessageChannelBinder.setLockRegistry(lockRegistry);
		kinesisMessageChannelBinder.setExtendedBindingProperties(kinesisExtendedBindingProperties);
		kinesisMessageChannelBinder.setKinesisProducerConfiguration(kinesisProducerConfiguration);
		kinesisMessageChannelBinder.setProducerMessageHandlerCustomizer(producerMessageHandlerCustomizer);
		kinesisMessageChannelBinder.setConsumerEndpointCustomizer(consumerEndpointCustomizer);
		kinesisMessageChannelBinder.setKinesisClientLibConfigurations(kinesisClientLibConfigurations);
		return kinesisMessageChannelBinder;
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(HealthIndicator.class)
	@ConditionalOnEnabledHealthIndicator("binders")
	protected static class KinesisBinderHealthIndicatorConfiguration {

		@Bean
		@ConditionalOnMissingBean(name = "kinesisBinderHealthIndicator")
		public KinesisBinderHealthIndicator kinesisBinderHealthIndicator(
				KinesisMessageChannelBinder kinesisMessageChannelBinder) {

			return new KinesisBinderHealthIndicator(kinesisMessageChannelBinder);
		}

	}

}

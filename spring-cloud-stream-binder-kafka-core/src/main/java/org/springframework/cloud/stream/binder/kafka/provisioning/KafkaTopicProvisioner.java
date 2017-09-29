/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.provisioning;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Callable;

import kafka.common.ErrorMapping;
import kafka.utils.ZkUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.security.JaasUtils;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.admin.AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.utils.KafkaTopicUtils;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Kafka implementation for {@link ProvisioningProvider}
 *
 * @author Soby Chacko
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Simon Flandergan
 */
public class KafkaTopicProvisioner implements ProvisioningProvider<ExtendedConsumerProperties<KafkaConsumerProperties>,
		ExtendedProducerProperties<KafkaProducerProperties>>, InitializingBean {

	private final Log logger = LogFactory.getLog(getClass());

	private final KafkaBinderConfigurationProperties configurationProperties;

	private final AdminUtilsOperation adminUtilsOperation;

	private RetryOperations metadataRetryOperations;

	public KafkaTopicProvisioner(KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties,
								AdminUtilsOperation adminUtilsOperation) {
		this.configurationProperties = kafkaBinderConfigurationProperties;
		this.adminUtilsOperation = adminUtilsOperation;
	}

	/**
	 * @param metadataRetryOperations the retry configuration
	 */
	public void setMetadataRetryOperations(RetryOperations metadataRetryOperations) {
		this.metadataRetryOperations = metadataRetryOperations;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (this.metadataRetryOperations == null) {
			RetryTemplate retryTemplate = new RetryTemplate();

			SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
			simpleRetryPolicy.setMaxAttempts(10);
			retryTemplate.setRetryPolicy(simpleRetryPolicy);

			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(100);
			backOffPolicy.setMultiplier(2);
			backOffPolicy.setMaxInterval(1000);
			retryTemplate.setBackOffPolicy(backOffPolicy);
			this.metadataRetryOperations = retryTemplate;
		}
	}

	@Override
	public ProducerDestination provisionProducerDestination(final String name, ExtendedProducerProperties<KafkaProducerProperties> properties) {
		if (this.logger.isInfoEnabled()) {
			this.logger.info("Using kafka topic for outbound: " + name);
		}
		KafkaTopicUtils.validateTopicName(name);
		createTopicsIfAutoCreateEnabledAndAdminUtilsPresent(name, properties.getPartitionCount(), false);
		if (this.configurationProperties.isAutoCreateTopics() && adminUtilsOperation != null) {
			final ZkUtils zkUtils = ZkUtils.apply(this.configurationProperties.getZkConnectionString(),
					this.configurationProperties.getZkSessionTimeout(),
					this.configurationProperties.getZkConnectionTimeout(),
					JaasUtils.isZkSecurityEnabled());
			int partitions = adminUtilsOperation.partitionSize(name, zkUtils);
			return new KafkaProducerDestination(name, partitions);
		}
		else {
			return new KafkaProducerDestination(name);
		}
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(final String name, final String group, ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		KafkaTopicUtils.validateTopicName(name);
		boolean anonymous = !StringUtils.hasText(group);
		Assert.isTrue(!anonymous || !properties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		if (properties.getInstanceCount() == 0) {
			throw new IllegalArgumentException("Instance count cannot be zero");
		}
		int partitionCount = properties.getInstanceCount() * properties.getConcurrency();
		createTopicsIfAutoCreateEnabledAndAdminUtilsPresent(name, partitionCount, properties.getExtension().isAutoRebalanceEnabled());
		if (this.configurationProperties.isAutoCreateTopics() && adminUtilsOperation != null) {
			final ZkUtils zkUtils = ZkUtils.apply(this.configurationProperties.getZkConnectionString(),
					this.configurationProperties.getZkSessionTimeout(),
					this.configurationProperties.getZkConnectionTimeout(),
					JaasUtils.isZkSecurityEnabled());
			int partitions = adminUtilsOperation.partitionSize(name, zkUtils);
			if (properties.getExtension().isEnableDlq() && !anonymous) {
				String dlqTopic = StringUtils.hasText(properties.getExtension().getDlqName()) ?
						properties.getExtension().getDlqName() : "error." + name + "." + group;
				createTopicAndPartitions(dlqTopic, partitions, properties.getExtension().isAutoRebalanceEnabled());
				return new KafkaConsumerDestination(name, partitions, dlqTopic);
			}
			return new KafkaConsumerDestination(name, partitions);
		}
		return new KafkaConsumerDestination(name);
	}

	private void createTopicsIfAutoCreateEnabledAndAdminUtilsPresent(final String topicName, final int partitionCount,
																	boolean tolerateLowerPartitionsOnBroker) {
		if (this.configurationProperties.isAutoCreateTopics() && adminUtilsOperation != null) {
			createTopicAndPartitions(topicName, partitionCount, tolerateLowerPartitionsOnBroker);
		}
		else if (this.configurationProperties.isAutoCreateTopics() && adminUtilsOperation == null) {
			this.logger.warn("Auto creation of topics is enabled, but Kafka AdminUtils class is not present on the classpath. " +
					"No topic will be created by the binder");
		}
		else if (!this.configurationProperties.isAutoCreateTopics()) {
			this.logger.info("Auto creation of topics is disabled.");
		}
	}

	/**
	 * Creates a Kafka topic if needed, or try to increase its partition count to the
	 * desired number.
	 */
	private void createTopicAndPartitions(final String topicName, final int partitionCount,
										boolean tolerateLowerPartitionsOnBroker) {

		final ZkUtils zkUtils = ZkUtils.apply(this.configurationProperties.getZkConnectionString(),
				this.configurationProperties.getZkSessionTimeout(),
				this.configurationProperties.getZkConnectionTimeout(),
				JaasUtils.isZkSecurityEnabled());
		try {
			short errorCode = adminUtilsOperation.errorCodeFromTopicMetadata(topicName, zkUtils);
			if (errorCode == ErrorMapping.NoError()) {
				// only consider minPartitionCount for resizing if autoAddPartitions is true
				int effectivePartitionCount = this.configurationProperties.isAutoAddPartitions()
						? Math.max(this.configurationProperties.getMinPartitionCount(), partitionCount)
						: partitionCount;
				int partitionSize = adminUtilsOperation.partitionSize(topicName, zkUtils);

				if (partitionSize < effectivePartitionCount) {
					if (this.configurationProperties.isAutoAddPartitions()) {
						adminUtilsOperation.invokeAddPartitions(zkUtils, topicName, effectivePartitionCount, null, false);
					}
					else if (tolerateLowerPartitionsOnBroker) {
						logger.warn("The number of expected partitions was: " + partitionCount + ", but "
								+ partitionSize + (partitionSize > 1 ? " have " : " has ") + "been found instead."
								+ "There will be " + (effectivePartitionCount - partitionSize) + " idle consumers");
					}
					else {
						throw new ProvisioningException("The number of expected partitions was: " + partitionCount + ", but "
								+ partitionSize + (partitionSize > 1 ? " have " : " has ") + "been found instead."
								+ "Consider either increasing the partition count of the topic or enabling " +
								"`autoAddPartitions`");
					}
				}
			}
			else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
				// always consider minPartitionCount for topic creation
				final int effectivePartitionCount = Math.max(this.configurationProperties.getMinPartitionCount(),
						partitionCount);

				this.metadataRetryOperations.execute(context -> {

					try {
						adminUtilsOperation.invokeCreateTopic(zkUtils, topicName, effectivePartitionCount,
								configurationProperties.getReplicationFactor(), new Properties());
					}
					catch (Exception e) {
						String exceptionClass = e.getClass().getName();
						if (exceptionClass.equals("kafka.common.TopicExistsException")
								|| exceptionClass.equals("org.apache.kafka.common.errors.TopicExistsException")) {
							if (logger.isWarnEnabled()) {
								logger.warn("Attempt to create topic: " + topicName + ". Topic already exists.");
							}
						}
						else {
							throw e;
						}
					}
					return null;
				});
			}
			else {
				throw new ProvisioningException("Error fetching Kafka topic metadata: ",
						ErrorMapping.exceptionFor(errorCode));
			}
		}
		finally {
			zkUtils.close();
		}
	}

	public Collection<PartitionInfo> getPartitionsForTopic(final int partitionCount,
														final boolean tolerateLowerPartitionsOnBroker,
														final Callable<Collection<PartitionInfo>> callable) {
		try {
			return this.metadataRetryOperations
					.execute(context -> {
						Collection<PartitionInfo> partitions = callable.call();
						// do a sanity check on the partition set
						int partitionSize = partitions.size();
						if (partitionSize < partitionCount) {
							if (tolerateLowerPartitionsOnBroker) {
								logger.warn("The number of expected partitions was: " + partitionCount + ", but "
										+ partitionSize + (partitionSize > 1 ? " have " : " has ") + "been found instead."
										+ "There will be " + (partitionCount - partitionSize) + " idle consumers");
							}
							else {
								throw new IllegalStateException("The number of expected partitions was: "
										+ partitionCount + ", but " + partitionSize
										+ (partitionSize > 1 ? " have " : " has ") + "been found instead");
							}
						}
						return partitions;
					});
		}
		catch (Exception e) {
			this.logger.error("Cannot initialize Binder", e);
			throw new BinderException("Cannot initialize binder:", e);
		}
	}

	private static final class KafkaProducerDestination implements ProducerDestination {

		private final String producerDestinationName;

		private final int partitions;

		KafkaProducerDestination(String destinationName) {
			this(destinationName, 0);
		}

		KafkaProducerDestination(String destinationName, Integer partitions) {
			this.producerDestinationName = destinationName;
			this.partitions = partitions;
		}

		@Override
		public String getName() {
			return producerDestinationName;
		}

		@Override
		public String getNameForPartition(int partition) {
			return producerDestinationName;
		}

		@Override
		public String toString() {
			return "KafkaProducerDestination{" +
					"producerDestinationName='" + producerDestinationName + '\'' +
					", partitions=" + partitions +
					'}';
		}
	}

	private static final class KafkaConsumerDestination implements ConsumerDestination {

		private final String consumerDestinationName;

		private final int partitions;

		private final String dlqName;

		KafkaConsumerDestination(String consumerDestinationName) {
			this(consumerDestinationName, 0, null);
		}

		KafkaConsumerDestination(String consumerDestinationName, int partitions) {
			this(consumerDestinationName, partitions, null);
		}

		KafkaConsumerDestination(String consumerDestinationName, Integer partitions, String dlqName) {
			this.consumerDestinationName = consumerDestinationName;
			this.partitions = partitions;
			this.dlqName = dlqName;
		}

		@Override
		public String getName() {
			return this.consumerDestinationName;
		}

		@Override
		public String toString() {
			return "KafkaConsumerDestination{" +
					"consumerDestinationName='" + consumerDestinationName + '\'' +
					", partitions=" + partitions +
					", dlqName='" + dlqName + '\'' +
					'}';
		}
	}
}

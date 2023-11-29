/*
 * Copyright 2014-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.provisioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.common.TopicInformation;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaTopicProperties;
import org.springframework.cloud.stream.binder.kafka.utils.KafkaTopicUtils;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Kafka implementation for {@link ProvisioningProvider}.
 *
 * @author Soby Chacko
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Simon Flandergan
 * @author Oleg Zhurakousky
 * @author Aldo Sinanaj
 * @author Yi Liu
 * @author Omer Celik
 * @author Byungjun You
 */
public class KafkaTopicProvisioner implements
		// @checkstyle:off
		ProvisioningProvider<ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>>,
		// @checkstyle:on
		InitializingBean {

	private static final Log logger = LogFactory.getLog(KafkaTopicProvisioner.class);

	private static final int DEFAULT_OPERATION_TIMEOUT = 30;

	private final KafkaBinderConfigurationProperties configurationProperties;

	private final int operationTimeout = DEFAULT_OPERATION_TIMEOUT;

	private final Map<String, Object> adminClientProperties;

	private RetryOperations metadataRetryOperations;

	/**
	 * Create an instance.
	 * @param kafkaBinderConfigurationProperties the binder configuration properties.
	 * @param kafkaProperties the boot Kafka properties used to build the
	 * @param adminClientConfigCustomizer to customize {@link AdminClient}.
	 * {@link AdminClient}.
	 */
	public KafkaTopicProvisioner(
			KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties,
			KafkaProperties kafkaProperties,
			AdminClientConfigCustomizer adminClientConfigCustomizer) {
		this(kafkaBinderConfigurationProperties, kafkaProperties, adminClientConfigCustomizer != null ?
				Arrays.asList(adminClientConfigCustomizer) : new ArrayList<>());
	}

	/**
	 * Create an instance.
	 *
	 * @param kafkaBinderConfigurationProperties the binder configuration properties.
	 * @param kafkaProperties the boot Kafka properties used to build the
	 * @param adminClientConfigCustomizers to customize {@link AdminClient}.
	 * {@link AdminClient}.
	 */
	public KafkaTopicProvisioner(
			KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties,
			KafkaProperties kafkaProperties,
			List<AdminClientConfigCustomizer> adminClientConfigCustomizers) {

		Assert.isTrue(kafkaProperties != null, "KafkaProperties cannot be null");
		this.configurationProperties = kafkaBinderConfigurationProperties;
		this.adminClientProperties = kafkaProperties.buildAdminProperties();
		normalalizeBootPropsWithBinder(this.adminClientProperties, kafkaProperties,
			kafkaBinderConfigurationProperties);
		// If the application provides AdminConfig customizers
		// and overrides properties, those take precedence.
		adminClientConfigCustomizers.forEach(customizer -> customizer.configure(this.adminClientProperties));
	}

	/**
	 * Return an unmodifiable map of merged admin properties.
	 * @return the properties.
	 * @since 4.0.3
	 */
	public Map<String, Object> getAdminClientProperties() {
		return Collections.unmodifiableMap(this.adminClientProperties);
	}

	/**
	 * Mutator for metadata retry operations.
	 * @param metadataRetryOperations the retry configuration
	 */
	public void setMetadataRetryOperations(RetryOperations metadataRetryOperations) {
		this.metadataRetryOperations = metadataRetryOperations;
	}

	@Override
	public void afterPropertiesSet() {
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
	public ProducerDestination provisionProducerDestination(final String name,
			ExtendedProducerProperties<KafkaProducerProperties> properties) {

		if (logger.isInfoEnabled()) {
			logger.info("Using kafka topic for outbound: " + name);
		}
		if (this.configurationProperties.isAutoCreateTopics()) {
			KafkaTopicUtils.validateTopicName(name);
			try (AdminClient adminClient = createAdminClient()) {
				createTopic(adminClient, name, properties.getPartitionCount(), false,
					properties.getExtension().getTopic());
				int partitions = getPartitionsForTopic(name, adminClient);
				return new KafkaProducerDestination(name, partitions);
			}
		}
		return new KafkaProducerDestination(name, 0);
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(final String name,
			final String group,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		if (!properties.isMultiplex()) {
			return doProvisionConsumerDestination(name, group, properties);
		}
		else {
			String[] destinations = StringUtils.commaDelimitedListToStringArray(name);
			for (String destination : destinations) {
				doProvisionConsumerDestination(destination.trim(), group, properties);
			}
			return new KafkaConsumerDestination(name);
		}
	}

	private ConsumerDestination doProvisionConsumerDestination(final String name,
			final String group,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		final KafkaConsumerDestination kafkaConsumerDestination = new KafkaConsumerDestination(name);
		if (properties.getExtension().isDestinationIsPattern()) {
			Assert.isTrue(!properties.getExtension().isEnableDlq(),
					"enableDLQ is not allowed when listening to topic patterns");
			if (logger.isDebugEnabled()) {
				logger.debug("Listening to a topic pattern - " + name
						+ " - no provisioning performed");
			}
			return kafkaConsumerDestination;
		}
		if (this.configurationProperties.isAutoCreateTopics()) {
			KafkaTopicUtils.validateTopicName(name);
			boolean anonymous = !StringUtils.hasText(group);
			Assert.isTrue(!anonymous || !properties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
			if (properties.getInstanceCount() == 0) {
				throw new IllegalArgumentException("Instance count cannot be zero");
			}
			int partitionCount = properties.getInstanceCount() * properties.getConcurrency();
			ConsumerDestination consumerDestination;
			try (AdminClient adminClient = createAdminClient()) {
				createTopic(adminClient, name, partitionCount,
					properties.getExtension().isAutoRebalanceEnabled(),
					properties.getExtension().getTopic());
				int partitions = getPartitionsForTopic(name, adminClient);
				consumerDestination = createDlqIfNeedBe(adminClient, name, group,
					properties, anonymous, partitions);
				if (consumerDestination == null) {
					consumerDestination = new KafkaConsumerDestination(name,
						partitions);
				}
				return consumerDestination;
			}
		}
		return kafkaConsumerDestination;
	}
	private int getPartitionsForTopic(String topicName, AdminClient adminClient) {
		int partitions = 0;
		Map<String, TopicDescription> topicDescriptions = retrieveTopicDescriptions(topicName, adminClient);
		TopicDescription topicDescription = topicDescriptions.get(topicName);
		if (topicDescription != null) {
			partitions = topicDescription.partitions().size();
		}
		return partitions;
	}

	private Map<String, TopicDescription> retrieveTopicDescriptions(String topicName, AdminClient adminClient) {
		return this.metadataRetryOperations.execute(context -> {
			try {
				if (logger.isDebugEnabled()) {
					logger.debug("Attempting to retrieve the description for the topic: " + topicName);
				}
				DescribeTopicsResult describeTopicsResult = adminClient
					.describeTopics(Collections.singletonList(topicName));
				KafkaFuture<Map<String, TopicDescription>> all = describeTopicsResult
					.allTopicNames();
				return all.get(this.operationTimeout, TimeUnit.SECONDS);
			}
			catch (Exception ex) {
				throw new ProvisioningException("Problems encountered with partitions finding for: " + topicName, ex);
			}
		});
	}

	AdminClient createAdminClient() {
		return AdminClient.create(this.adminClientProperties);
	}

	/**
	 * In general, binder properties supersede boot kafka properties. The one exception is
	 * the bootstrap servers. In that case, we should only override the boot properties if
	 * (there is a binder property AND it is a non-default value) OR (if there is no boot
	 * property); this is needed because the binder property never returns a null value.
	 * @param adminProps the admin properties to normalize.
	 * @param bootProps the boot kafka properties.
	 * @param binderProps the binder kafka properties.
	 */
	public static void normalalizeBootPropsWithBinder(Map<String, Object> adminProps,
			KafkaProperties bootProps, KafkaBinderConfigurationProperties binderProps) {
		// First deal with the outlier
		String kafkaConnectionString = binderProps.getKafkaConnectionString();
		if (ObjectUtils
				.isEmpty(adminProps.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG))
				|| !kafkaConnectionString
				.equals(binderProps.getDefaultKafkaConnectionString())) {
			adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
					kafkaConnectionString);
		}
		// Now override any boot values with binder values
		Map<String, String> binderProperties = binderProps.getConfiguration();
		Set<String> adminConfigNames = AdminClientConfig.configNames();
		binderProperties.forEach((key, value) -> {
			if (key.equals(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
				throw new IllegalStateException(
						"Set binder bootstrap servers via the 'brokers' property, not 'configuration'");
			}
			if (adminConfigNames.contains(key)) {
				Object replaced = adminProps.put(key, value);
				if (replaced != null && KafkaTopicProvisioner.logger.isDebugEnabled()) {
					KafkaTopicProvisioner.logger.debug("Overrode boot property: [" + key + "], from: ["
							+ replaced + "] to: [" + value + "]");
				}
			}
		});
	}

	private ConsumerDestination createDlqIfNeedBe(AdminClient adminClient, String name,
			String group, ExtendedConsumerProperties<KafkaConsumerProperties> properties,
			boolean anonymous, int partitions) {

		if (properties.getExtension().isEnableDlq() && !anonymous) {
			String dlqTopic = StringUtils.hasText(properties.getExtension().getDlqName())
					? properties.getExtension().getDlqName()
					: "error." + name + "." + group;
			int dlqPartitions = properties.getExtension().getDlqPartitions() == null
					? partitions
					: properties.getExtension().getDlqPartitions();
			try {
				final KafkaProducerProperties dlqProducerProperties = properties.getExtension().getDlqProducerProperties();
				createTopicAndPartitions(adminClient, dlqTopic, dlqPartitions,
						properties.getExtension().isAutoRebalanceEnabled(),
						dlqProducerProperties.getTopic());
			}
			catch (Throwable throwable) {
				if (throwable instanceof Error throwableError) {
					throw throwableError;
				}
				else {
					throw new ProvisioningException("Provisioning exception encountered for " + name, throwable);
				}
			}
			return new KafkaConsumerDestination(name, partitions, dlqTopic);
		}
		return null;
	}

	private void createTopic(AdminClient adminClient, String name, int partitionCount,
			boolean tolerateLowerPartitionsOnBroker, KafkaTopicProperties properties) {
		try {
			createTopicIfNecessary(adminClient, name, partitionCount,
					tolerateLowerPartitionsOnBroker, properties);
		}
		// TODO: Remove catching Throwable. See this thread:
		// TODO:
		// https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/pull/514#discussion_r241075940
		catch (Throwable throwable) {
			if (throwable instanceof Error throwableError) {
				throw throwableError;
			}
			else {
				// TODO:
				// https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/pull/514#discussion_r241075940
				throw new ProvisioningException("Provisioning exception encountered for " + name, throwable);
			}
		}
	}

	private void createTopicIfNecessary(AdminClient adminClient, final String topicName,
			final int partitionCount, boolean tolerateLowerPartitionsOnBroker,
			KafkaTopicProperties properties) throws Throwable {

		if (this.configurationProperties.isAutoCreateTopics()) {
			createTopicAndPartitions(adminClient, topicName, partitionCount,
					tolerateLowerPartitionsOnBroker, properties);
		}
		else {
			logger.info("Auto creation of topics is disabled.");
		}
	}

	/**
	 * Creates a Kafka topic if needed, or try to increase its partition count to the
	 * desired number.
	 * @param adminClient kafka admin client
	 * @param topicName topic name
	 * @param partitionCount partition count
	 * @param tolerateLowerPartitionsOnBroker whether lower partitions count on broker is
	 * tolerated ot not
	 * @param topicProperties kafka topic properties
	 * @throws Throwable from topic creation
	 */
	private void createTopicAndPartitions(AdminClient adminClient, final String topicName,
			final int partitionCount, boolean tolerateLowerPartitionsOnBroker,
			KafkaTopicProperties topicProperties) throws Throwable {

		ListTopicsResult listTopicsResult = adminClient.listTopics();
		KafkaFuture<Set<String>> namesFutures = listTopicsResult.names();

		Set<String> names = namesFutures.get(this.operationTimeout, TimeUnit.SECONDS);
		if (names.contains(topicName)) {
			//check if topic.properties are different from Topic Configuration in Kafka
			if (this.configurationProperties.isAutoAlterTopics()) {
				alterTopicConfigsIfNecessary(adminClient, topicName, topicProperties);
			}
			// only consider minPartitionCount for resizing if autoAddPartitions is true
			int effectivePartitionCount = this.configurationProperties
					.isAutoAddPartitions()
					? Math.max(
					this.configurationProperties.getMinPartitionCount(),
					partitionCount)
					: partitionCount;
			DescribeTopicsResult describeTopicsResult = adminClient
					.describeTopics(Collections.singletonList(topicName));
			KafkaFuture<Map<String, TopicDescription>> topicDescriptionsFuture = describeTopicsResult
					.all();
			Map<String, TopicDescription> topicDescriptions = topicDescriptionsFuture
					.get(this.operationTimeout, TimeUnit.SECONDS);
			TopicDescription topicDescription = topicDescriptions.get(topicName);
			int partitionSize = topicDescription.partitions().size();
			if (partitionSize < effectivePartitionCount) {
				if (this.configurationProperties.isAutoAddPartitions()) {
					CreatePartitionsResult partitions = adminClient
							.createPartitions(Collections.singletonMap(topicName,
									NewPartitions.increaseTo(effectivePartitionCount)));
					partitions.all().get(this.operationTimeout, TimeUnit.SECONDS);
				}
				else if (tolerateLowerPartitionsOnBroker) {
					logger.warn("The number of expected partitions for topic "
							+ topicName + " was: "
							+ partitionCount + ", but " + partitionSize
							+ (partitionSize > 1 ? " have " : " has ")
							+ "been found instead. " + "There will be "
							+ (effectivePartitionCount - partitionSize)
							+ " idle consumers");
				}
				else {
					throw new ProvisioningException(
							"The number of expected partitions for topic " + topicName
									+ " was: " + partitionCount
									+ ", but " + partitionSize
									+ (partitionSize > 1 ? " have " : " has ")
									+ "been found instead. "
									+ "Consider either increasing the partition count of the topic or enabling "
									+ "`autoAddPartitions`");
				}
			}
		}
		else {
			// always consider minPartitionCount for topic creation
			final int effectivePartitionCount = Math.max(
					this.configurationProperties.getMinPartitionCount(), partitionCount);
			this.metadataRetryOperations.execute((context) -> {

				NewTopic newTopic;
				Map<Integer, List<Integer>> replicasAssignments = topicProperties
						.getReplicasAssignments();
				if (replicasAssignments != null && replicasAssignments.size() > 0) {
					newTopic = new NewTopic(topicName,
							topicProperties.getReplicasAssignments());
				}
				else {
					newTopic = new NewTopic(topicName, effectivePartitionCount,
							topicProperties.getReplicationFactor() != null
									? topicProperties.getReplicationFactor()
									: this.configurationProperties
									.getReplicationFactor());
				}
				if (topicProperties.getProperties().size() > 0) {
					newTopic.configs(topicProperties.getProperties());
				}
				CreateTopicsResult createTopicsResult = adminClient
						.createTopics(Collections.singletonList(newTopic));
				try {
					createTopicsResult.all().get(this.operationTimeout, TimeUnit.SECONDS);
				}
				catch (Exception ex) {
					if (ex instanceof ExecutionException) {
						if (ex.getCause() instanceof TopicExistsException) {
							if (logger.isWarnEnabled()) {
								logger.warn("Attempt to create topic: " + topicName
										+ ". Topic already exists.");
							}
						}
						else {
							logger.error("Failed to create topics", ex.getCause());
							throw ex.getCause();
						}
					}
					else {
						logger.error("Failed to create topics", ex.getCause());
						throw ex.getCause();
					}
				}
				return null;
			});
		}
	}

	private void alterTopicConfigsIfNecessary(AdminClient adminClient,
											String topicName,
											KafkaTopicProperties topicProperties)
			throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
		ConfigResource topicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
		DescribeConfigsResult describeConfigsResult = adminClient
				.describeConfigs(Collections.singletonList(topicConfigResource));
		KafkaFuture<Map<ConfigResource, Config>> topicConfigurationFuture = describeConfigsResult.all();
		Map<ConfigResource, Config> topicConfigMap = topicConfigurationFuture
				.get(this.operationTimeout, TimeUnit.SECONDS);
		Config config = topicConfigMap.get(topicConfigResource);
		final List<AlterConfigOp> updatedConfigEntries = topicProperties.getProperties().entrySet().stream()
				.filter(propertiesEntry -> {
					// Property is new and should be added
					if (config.get(propertiesEntry.getKey()) == null) {
						return true;
					}
					else {
						// Property changed and should be updated
						return !config.get(propertiesEntry.getKey()).value().equals(propertiesEntry.getValue());
					}

				})
				.map(propertyEntry -> new ConfigEntry(propertyEntry.getKey(), propertyEntry.getValue()))
				.map(configEntry -> new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET))
				.collect(Collectors.toList());
		if (!updatedConfigEntries.isEmpty()) {
			if (logger.isDebugEnabled()) {
				logger.debug("Attempting to alter configs " + updatedConfigEntries + " for the topic:" + topicName);
			}
			Map<ConfigResource, Collection<AlterConfigOp>> alterConfigForTopics = new HashMap<>();
			alterConfigForTopics.put(topicConfigResource, updatedConfigEntries);
			AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(alterConfigForTopics);
			alterConfigsResult.all().get(this.operationTimeout, TimeUnit.SECONDS);
		}
	}

	public Collection<PartitionInfo> getListenedPartitions(final String group,
		final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
		final ConsumerFactory<?, ?> consumerFactory, int partitionCount,
		boolean usingPatterns, boolean groupManagement, final String topic,
		Map<String, TopicInformation> topicsInUse) {
		Collection<PartitionInfo> listenedPartitions;
		Collection<PartitionInfo> allPartitions = usingPatterns ? Collections.emptyList()
			: getPartitionInfoForConsumer(topic, extendedConsumerProperties, consumerFactory,
			partitionCount);

		if (groupManagement || extendedConsumerProperties.getInstanceCount() == 1) {
			listenedPartitions = allPartitions;
		}
		else {
			listenedPartitions = new ArrayList<>();
			for (PartitionInfo partition : allPartitions) {
				// divide partitions across modules
				if ((partition.partition() % extendedConsumerProperties
					.getInstanceCount()) == extendedConsumerProperties
					.getInstanceIndex()) {
					listenedPartitions.add(partition);
				}
			}
		}
		topicsInUse.put(topic,
			new TopicInformation(group, listenedPartitions, usingPatterns));
		return listenedPartitions;
	}

	/**
	 * Check that the topic has the expected number of partitions and return the partition information for consumer.
	 */
	public Collection<PartitionInfo> getPartitionInfoForConsumer(final String topic,
		final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
		final ConsumerFactory<?, ?> consumerFactory, int partitionCount) {
		return getPartitionsForTopic(partitionCount,
			extendedConsumerProperties.getExtension().isAutoRebalanceEnabled(),
			() -> {
				try (Consumer<?, ?> consumer = consumerFactory.createConsumer()) {
					return consumer.partitionsFor(topic);
				}
			}, topic);
	}

	/**
	 * Check that the topic has the expected number of partitions and return the partition information for producer.
	 */
	public Collection<PartitionInfo> getPartitionInfoForProducer(final String topicName,
		final ProducerFactory<byte[], byte[]> producerFB,
		final ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
		return getPartitionsForTopic(
			producerProperties.getPartitionCount(), false, () -> {
				Producer<byte[], byte[]> producer = producerFB.createProducer();
				List<PartitionInfo> partitionsFor = producer
					.partitionsFor(topicName);
				producer.close();
				return partitionsFor;
			}, topicName);
	}

	/**
	 * Check that the topic has the expected number of partitions and return the partition information.
	 * @param partitionCount the expected count.
	 * @param tolerateLowerPartitionsOnBroker if false, throw an exception if there are not enough partitions.
	 * @param callable a Callable that will provide the partition information.
	 * @param topicName the topic./
	 * @return the partition information.
	 */
	public Collection<PartitionInfo> getPartitionsForTopic(final int partitionCount,
			final boolean tolerateLowerPartitionsOnBroker,
			final Callable<Collection<PartitionInfo>> callable, final String topicName) {
		try {
			return this.metadataRetryOperations.execute((context) -> {
				Collection<PartitionInfo> partitions = Collections.emptyList();

				try {
					// This call may return null or throw an exception.
					partitions = callable.call();
				}
				catch (Exception ex) {
					// The above call can potentially throw exceptions such as timeout. If
					// we can determine
					// that the exception was due to an unknown topic on the broker, just
					// simply rethrow that.
					if (ex instanceof UnknownTopicOrPartitionException) {
						throw ex;
					}
					logger.error("Failed to obtain partition information for the topic "
						+ "(" + topicName + ").", ex);
				}
				// In some cases, the above partition query may not throw an UnknownTopic..Exception for various reasons.
				// For that, we are forcing another query to ensure that the topic is present on the server.
				if (CollectionUtils.isEmpty(partitions)) {
					try (AdminClient adminClient = createAdminClient()) {
						final DescribeTopicsResult describeTopicsResult = adminClient
								.describeTopics(Collections.singletonList(topicName));

						describeTopicsResult.all().get();
					}
					catch (ExecutionException ex) {
						if (ex.getCause() instanceof UnknownTopicOrPartitionException unknownTopicOrPartitionException) {
							throw unknownTopicOrPartitionException;
						}
						else {
							logger.warn("No partitions have been retrieved for the topic "
									+ "(" + topicName
									+ "). This will affect the health check.");
						}
					}
				}
				// do a sanity check on the partition set
				int partitionSize = CollectionUtils.isEmpty(partitions) ? 0 : partitions.size();
				if (partitionSize < partitionCount) {
					if (tolerateLowerPartitionsOnBroker) {
						logger.warn("The number of expected partitions for topic "
								+ topicName + " was: "
								+ partitionCount + ", but " + partitionSize
								+ (partitionSize > 1 ? " have " : " has ")
								+ "been found instead. " + "There will be "
								+ (partitionCount - partitionSize) + " idle consumers");
					}
					else {
						throw new IllegalStateException(
								"The number of expected partitions for topic " + topicName
									+ " was: " + partitionCount
										+ ", but " + partitionSize
										+ (partitionSize > 1 ? " have " : " has ")
										+ "been found instead");
					}
				}
				return partitions;
			});
		}
		catch (Exception ex) {
			logger.error("Cannot initialize Binder checking the topic (" + topicName + ").", ex);
			throw new BinderException("Cannot initialize binder checking the topic (" + topicName + "):", ex);
		}
	}

	private static final class KafkaProducerDestination implements ProducerDestination {

		private final String producerDestinationName;

		private final int partitions;

		KafkaProducerDestination(String destinationName, Integer partitions) {
			this.producerDestinationName = destinationName;
			this.partitions = partitions;
		}

		@Override
		public String getName() {
			return this.producerDestinationName;
		}

		@Override
		public String getNameForPartition(int partition) {
			return this.producerDestinationName;
		}

		@Override
		public String toString() {
			return "KafkaProducerDestination{" + "producerDestinationName='"
					+ producerDestinationName + '\'' + ", partitions=" + partitions + '}';
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

		KafkaConsumerDestination(String consumerDestinationName, Integer partitions,
				String dlqName) {
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
			return "KafkaConsumerDestination{" + "consumerDestinationName='"
					+ consumerDestinationName + '\'' + ", partitions=" + partitions
					+ ", dlqName='" + dlqName + '\'' + '}';
		}

	}

}

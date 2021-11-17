/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.StreamsMetadata;

import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.StringUtils;

/**
 * Services pertinent to the interactive query capabilities of Kafka Streams. This class
 * provides services such as querying for a particular store, which instance is hosting a
 * particular store etc. This is part of the public API of the kafka streams binder and
 * the users can inject this service in their applications to make use of it.
 *
 * @author Soby Chacko
 * @author Renwei Han
 * @author Serhii Siryi
 * @author Nico Pommerening
 * @since 2.1.0
 */
public class InteractiveQueryService {

	private static final Log LOG = LogFactory.getLog(InteractiveQueryService.class);

	private final KafkaStreamsRegistry kafkaStreamsRegistry;

	private final KafkaStreamsBinderConfigurationProperties binderConfigurationProperties;

	/**
	 * Constructor for InteractiveQueryService.
	 * @param kafkaStreamsRegistry holding {@link KafkaStreamsRegistry}
	 * @param binderConfigurationProperties kafka Streams binder configuration properties
	 */
	public InteractiveQueryService(KafkaStreamsRegistry kafkaStreamsRegistry,
			KafkaStreamsBinderConfigurationProperties binderConfigurationProperties) {
		this.kafkaStreamsRegistry = kafkaStreamsRegistry;
		this.binderConfigurationProperties = binderConfigurationProperties;
	}

	/**
	 * Retrieve and return a queryable store by name created in the application.
	 * @param storeName name of the queryable store
	 * @param storeType type of the queryable store
	 * @param <T> generic queryable store
	 * @return queryable store.
	 */
	public <T> T getQueryableStore(String storeName, QueryableStoreType<T> storeType) {

		RetryTemplate retryTemplate = new RetryTemplate();

		KafkaStreamsBinderConfigurationProperties.StateStoreRetry stateStoreRetry = this.binderConfigurationProperties.getStateStoreRetry();
		RetryPolicy retryPolicy = new SimpleRetryPolicy(stateStoreRetry.getMaxAttempts());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(stateStoreRetry.getBackoffPeriod());

		retryTemplate.setBackOffPolicy(backOffPolicy);
		retryTemplate.setRetryPolicy(retryPolicy);

		KafkaStreams contextSpecificKafkaStreams = getThreadContextSpecificKafkaStreams();

		return retryTemplate.execute(context -> {
			T store = null;
			Throwable throwable = null;
			if (contextSpecificKafkaStreams != null) {
				try {
					store = contextSpecificKafkaStreams.store(
							StoreQueryParameters.fromNameAndType(
									storeName, storeType));
				}
				catch (InvalidStateStoreException e) {
					// pass through..
					throwable = e;
				}
			}
			if (store != null) {
				return store;
			}
			else if (contextSpecificKafkaStreams != null) {
				LOG.warn("Store " + storeName
						+ " could not be found in Streams context, falling back to all known Streams instances");
			}
			final Set<KafkaStreams> kafkaStreams = kafkaStreamsRegistry.getKafkaStreams();
			final Iterator<KafkaStreams> iterator = kafkaStreams.iterator();
			while (iterator.hasNext()) {
				try {
					store = iterator.next()
							.store(StoreQueryParameters.fromNameAndType(
									storeName, storeType));
				}
				catch (InvalidStateStoreException e) {
					// pass through..
					throwable = e;
				}
			}
			if (store != null) {
				return store;
			}
			throw new IllegalStateException(
					"Error when retrieving state store: " + storeName,
					throwable);
		});
	}

	/**
	 * Retrieves the current {@link KafkaStreams} context if executing Thread is created by a Streams App (contains a matching application id in Thread's name).
	 *
	 * @return KafkaStreams instance associated with Thread
	 */
	private KafkaStreams getThreadContextSpecificKafkaStreams() {
		return this.kafkaStreamsRegistry.getKafkaStreams().stream()
				.filter(this::filterByThreadName).findAny().orElse(null);
	}

	/**
	 * Checks if the supplied {@link KafkaStreams} instance belongs to the calling Thread by matching the Thread's name with the Streams Application Id.
	 *
	 * @param streams {@link KafkaStreams} instance to filter
	 * @return true if Streams Instance is associated with Thread
	 */
	private boolean filterByThreadName(KafkaStreams streams) {
		String applicationId = kafkaStreamsRegistry.streamBuilderFactoryBean(
						streams).getStreamsConfiguration()
				.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
		// TODO: is there some better way to find out if a Stream App created the Thread?
		return Thread.currentThread().getName().contains(applicationId);
	}

	/**
	 * Gets the current {@link HostInfo} that the calling kafka streams application is
	 * running on.
	 *
	 * Note that the end user applications must provide `applicaiton.server` as a
	 * configuration property when calling this method. If this is not available, then
	 * null is returned.
	 * @return the current {@link HostInfo}
	 */
	public HostInfo getCurrentHostInfo() {
		Map<String, String> configuration = this.binderConfigurationProperties
				.getConfiguration();
		if (configuration.containsKey("application.server")) {

			String applicationServer = configuration.get("application.server");
			String[] splits = StringUtils.split(applicationServer, ":");

			return new HostInfo(splits[0], Integer.valueOf(splits[1]));
		}
		return null;
	}

	/**
	 * Gets the {@link HostInfo} where the provided store and key are hosted on. This may
	 * not be the current host that is running the application. Kafka Streams will look
	 * through all the consumer instances under the same application id and retrieves the
	 * proper host.
	 *
	 * Note that the end user applications must provide `applicaiton.server` as a
	 * configuration property for all the application instances when calling this method.
	 * If this is not available, then null maybe returned.
	 * @param <K> generic type for key
	 * @param store store name
	 * @param key key to look for
	 * @param serializer {@link Serializer} for the key
	 * @return the {@link HostInfo} where the key for the provided store is hosted currently
	 */
	public <K> HostInfo getHostInfo(String store, K key, Serializer<K> serializer) {
		final KeyQueryMetadata keyQueryMetadata = this.kafkaStreamsRegistry.getKafkaStreams()
				.stream()
				.map((k) -> Optional.ofNullable(k.queryMetadataForKey(store, key, serializer)))
				.filter(Optional::isPresent).map(Optional::get).findFirst().orElse(null);
		return keyQueryMetadata != null ? keyQueryMetadata.getActiveHost() : null;
	}

	/**
	 * Retrieves and returns the {@link KeyQueryMetadata} associated with the given combination of
	 * key and state store. If none found, it will return null.
	 *
	 * @param <K> generic type for key
	 * @param store store name
	 * @param key key to look for
	 * @param serializer {@link Serializer} for the key
	 * @return the {@link KeyQueryMetadata} if available, null otherwise.
	 */
	public <K> KeyQueryMetadata getKeyQueryMetadata(String store, K key, Serializer<K> serializer) {
		return this.kafkaStreamsRegistry.getKafkaStreams()
				.stream()
				.map((k) -> Optional.ofNullable(k.queryMetadataForKey(store, key, serializer)))
				.filter(Optional::isPresent).map(Optional::get).findFirst().orElse(null);
	}

	/**
	 * Retrieves and returns the {@link KafkaStreams} object that is associated with the given combination of
	 * key and state store. If none found, it will return null.
	 *
	 * @param <K> generic type for key
	 * @param store store name
	 * @param key key to look for
	 * @param serializer {@link Serializer} for the key
	 * @return {@link KafkaStreams} object associated with this combination of store and key
	 */
	public <K> KafkaStreams getKafkaStreams(String store, K key, Serializer<K> serializer) {
		final AtomicReference<KafkaStreams> kafkaStreamsAtomicReference = new AtomicReference<>();
		this.kafkaStreamsRegistry.getKafkaStreams()
				.forEach(k -> {
					final KeyQueryMetadata keyQueryMetadata = k.queryMetadataForKey(store, key, serializer);
					if (keyQueryMetadata != null) {
						kafkaStreamsAtomicReference.set(k);
					}
				});
		return kafkaStreamsAtomicReference.get();
	}

	/**
	 * Gets the list of {@link HostInfo} where the provided store is hosted on.
	 * It also can include current host info.
	 * Kafka Streams will look through all the consumer instances under the same application id
	 * and retrieves all hosts info.
	 *
	 * Note that the end-user applications must provide `application.server` as a configuration property
	 * for all the application instances when calling this method. If this is not available, then an empty list will be returned.
	 *
	 * @param store store name
	 * @return the list of {@link HostInfo} where provided store is hosted on
	 */
	public List<HostInfo> getAllHostsInfo(String store) {
		return kafkaStreamsRegistry.getKafkaStreams()
				.stream()
				.flatMap(k -> k.allMetadataForStore(store).stream())
				.filter(Objects::nonNull)
				.map(StreamsMetadata::hostInfo)
				.collect(Collectors.toList());
	}

}

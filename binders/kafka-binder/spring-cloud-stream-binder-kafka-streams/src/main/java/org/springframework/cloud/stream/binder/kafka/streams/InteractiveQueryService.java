/*
 * Copyright 2018-present the original author or authors.
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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;

import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Services pertinent to the interactive query capabilities of Kafka Streams. This class
 * provides services such as querying for a particular store, which instance is hosting a
 * particular store etc. This is part of the public API of the kafka streams binder, and
 * the users can inject this service in their applications to make use of it.
 *
 * @author Soby Chacko
 * @author Renwei Han
 * @author Serhii Siryi
 * @author Nico Pommerening
 * @author Chris Bono
 * @author Artem Bilan
 * @since 2.1.0
 */
public class InteractiveQueryService {

	private static final Log LOG = LogFactory.getLog(InteractiveQueryService.class);

	private final KafkaStreamsRegistry kafkaStreamsRegistry;

	private final KafkaStreamsBinderConfigurationProperties binderConfigurationProperties;

	private final KafkaStreamsVersionAgnosticTopologyInfoFacade topologyInfoFacade;

	private StoreQueryParametersCustomizer<?> storeQueryParametersCustomizer;

	/**
	 * Constructor for InteractiveQueryService.
	 * @param kafkaStreamsRegistry holding {@link KafkaStreamsRegistry}
	 * @param binderConfigurationProperties kafka Streams binder configuration properties
	 */
	public InteractiveQueryService(KafkaStreamsRegistry kafkaStreamsRegistry,
			KafkaStreamsBinderConfigurationProperties binderConfigurationProperties) {
		this.kafkaStreamsRegistry = kafkaStreamsRegistry;
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.topologyInfoFacade = new KafkaStreamsVersionAgnosticTopologyInfoFacade();
	}

	/**
	 * Retrieve and return a queryable store by name created in the application.
	 * @param storeName name of the queryable store
	 * @param storeType type of the queryable store
	 * @param <T> generic queryable store
	 * @return queryable store.
	 */
	@SuppressWarnings("unchecked")
	public <T> T getQueryableStore(String storeName, QueryableStoreType<T> storeType) {

		KafkaStreams contextSpecificKafkaStreams = getThreadContextSpecificKafkaStreams();

		StoreQueryParameters<T> storeQueryParams = StoreQueryParameters.fromNameAndType(storeName, storeType);
		if (this.storeQueryParametersCustomizer != null) {
			storeQueryParams = ((StoreQueryParametersCustomizer<T>) this.storeQueryParametersCustomizer).customize(storeQueryParams);
		}

		AtomicReference<StoreQueryParameters<T>> storeQueryParametersAtomicReference = new AtomicReference<>(storeQueryParams);

		try {
			return getRetryTemplate().execute(() -> {
				T store = null;
				Throwable throwable = null;
				if (contextSpecificKafkaStreams != null) {
					try {
						store = contextSpecificKafkaStreams.store(storeQueryParametersAtomicReference.get());
					}
					catch (InvalidStateStoreException e) {
						throwable = e;
					}
				}
				if (store != null) {
					return store;
				}
				if (contextSpecificKafkaStreams != null) {
					LOG.warn("Store (" + storeName + ") could not be found in Streams context, falling back to all known Streams instances");
				}

				// Find all apps that know about the store
				Map<KafkaStreams, T> candidateStores = new HashMap<>();
				for (KafkaStreams kafkaStreamApp : kafkaStreamsRegistry.getKafkaStreams()) {
					try {
						candidateStores.put(kafkaStreamApp, kafkaStreamApp.store(storeQueryParametersAtomicReference.get()));
					}
					catch (Exception ex) {
						throwable = ex;
					}
				}

				// Store exists in a single app - no further resolution required
				if (candidateStores.size() == 1) {
					return candidateStores.values().stream().findFirst().get();
				}

				// If the store is in multiple streams apps - discard any apps that do not actually have the store
				if (candidateStores.size() > 1) {

					candidateStores = candidateStores.entrySet().stream()
							.filter((e) -> this.topologyInfoFacade.streamsAppActuallyHasStore(e.getKey(), storeName))
							.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

					if (candidateStores.size() == 1) {
						return candidateStores.values().stream().findFirst().get();
					}

					throwable = (candidateStores.isEmpty()) ?
							new UnknownStateStoreException("Store (" + storeName + ") not available to Streams instance") :
							new InvalidStateStoreException("Store (" + storeName + ") available to more than one Streams instance");

				}
				throw new IllegalStateException("Error retrieving state store: " + storeName, throwable);
			});
		}
		catch (RetryException ex) {
			ReflectionUtils.rethrowRuntimeException(ex.getCause());
			return null;
		}
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
		String applicationId = Objects.requireNonNull(kafkaStreamsRegistry.streamBuilderFactoryBean(
				streams).getStreamsConfiguration())
				.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
		// TODO: is there some better way to find out if a Stream App created the Thread?
		return Thread.currentThread().getName().contains(applicationId);
	}

	/**
	 * Gets the current {@link HostInfo} that the calling kafka streams application is
	 * running on.
	 *
	 * Note that the end user applications must provide `application.server` as a
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

			return new HostInfo(Objects.requireNonNull(splits)[0], Integer.parseInt(splits[1]));
		}
		return null;
	}

	/**
	 * Gets the {@link HostInfo} where the provided store and key are hosted on. This may
	 * not be the current host that is running the application. Kafka Streams will look
	 * through all the consumer instances under the same application id and retrieves the
	 * proper host.
	 *
	 * Note that the end user applications must provide `application.server` as a
	 * configuration property for all the application instances when calling this method.
	 * If this is not available, then null maybe returned.
	 * @param <K> generic type for key
	 * @param store store name
	 * @param key key to look for
	 * @param serializer {@link Serializer} for the key
	 * @return the {@link HostInfo} where the key for the provided store is hosted currently
	 */
	public <K> HostInfo getHostInfo(String store, K key, Serializer<K> serializer) {
		final RetryTemplate retryTemplate = getRetryTemplate();

		try {
			return retryTemplate.execute(() -> {
				Throwable throwable = null;
				try {
					final KeyQueryMetadata keyQueryMetadata = this.kafkaStreamsRegistry.getKafkaStreams()
							.stream()
							.map((k) -> Optional.ofNullable(k.queryMetadataForKey(store, key, serializer)))
							.filter(Optional::isPresent).map(Optional::get).findFirst().orElse(null);
					if (keyQueryMetadata != null) {
						return keyQueryMetadata.activeHost();
					}
				}
				catch (Exception e) {
					throwable = e;
				}
				throw new IllegalStateException(
						"Error when retrieving state store.", throwable != null ? throwable : new Throwable("Kafka Streams is not ready."));
			});
		}
		catch (RetryException ex) {
			ReflectionUtils.rethrowRuntimeException(ex.getCause());
			return null;
		}
	}

	private RetryTemplate getRetryTemplate() {

		var stateStoreRetry = this.binderConfigurationProperties.getStateStoreRetry();
		RetryPolicy retryPolicy = RetryPolicy.builder()
			.maxRetries(stateStoreRetry.getMaxAttempts())
			.delay(Duration.ofMillis(stateStoreRetry.getBackoffPeriod()))
			.build();

		return new RetryTemplate(retryPolicy);
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
				.flatMap(k -> k.streamsMetadataForStore(store).stream())
				.filter(Objects::nonNull)
				.map(StreamsMetadata::hostInfo)
				.collect(Collectors.toList());
	}

	/**
	 * @param storeQueryParametersCustomizer to customize
	 * @since 4.0.1
	 */
	public void setStoreQueryParametersCustomizer(StoreQueryParametersCustomizer<?> storeQueryParametersCustomizer) {
		this.storeQueryParametersCustomizer = storeQueryParametersCustomizer;
	}
}

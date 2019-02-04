/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.StreamsMetadata;

import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * Services pertinent to the interactive query capabilities of Kafka Streams. This class
 * provides services such as querying for a particular store, which instance is hosting a
 * particular store etc. This is part of the public API of the kafka streams binder and
 * the users can inject this service in their applications to make use of it.
 *
 * @author Soby Chacko
 * @author Renwei Han
 * @since 2.1.0
 */
public class InteractiveQueryService {

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
		for (KafkaStreams kafkaStream : this.kafkaStreamsRegistry.getKafkaStreams()) {
			try {
				T store = kafkaStream.store(storeName, storeType);
				if (store != null) {
					return store;
				}
			}
			catch (InvalidStateStoreException ignored) {
				// pass through
			}
		}
		return null;
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
	 * @return the {@link HostInfo} where the key for the provided store is hosted
	 * currently
	 */
	public <K> HostInfo getHostInfo(String store, K key, Serializer<K> serializer) {
		StreamsMetadata streamsMetadata = this.kafkaStreamsRegistry.getKafkaStreams()
				.stream()
				.map((k) -> Optional.ofNullable(k.metadataForKey(store, key, serializer)))
				.filter(Optional::isPresent).map(Optional::get).findFirst().orElse(null);
		return streamsMetadata != null ? streamsMetadata.hostInfo() : null;
	}

}

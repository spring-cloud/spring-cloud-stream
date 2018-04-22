/*
 * Copyright 2018 the original author or authors.
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

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;

/**
 * Registry that contains {@link QueryableStoreType}s those created from
 * the user applications.
 *
 * @author Soby Chacko
 * @author Renwei Han
 * @since 2.0.0
 */
public class QueryableStoreRegistry {

	private final Set<KafkaStreams> kafkaStreams = new HashSet<>();

	/**
	 * Retrieve and return a queryable store by name created in the application.
	 *
	 * @param storeName name of the queryable store
	 * @param storeType type of the queryable store
	 * @param <T>       generic queryable store
	 * @return queryable store.
	 */
	public <T> T getQueryableStoreType(String storeName, QueryableStoreType<T> storeType) {

		for (KafkaStreams kafkaStream : kafkaStreams) {
			try{
				T store = kafkaStream.store(storeName, storeType);
				if (store != null) {
					return store;
				}
			}
			catch (InvalidStateStoreException ignored) {
				//pass through
			}
		}
		return null;
	}

	/**
	 * Register the {@link KafkaStreams} object created in the application.
	 *
	 * @param kafkaStreams {@link KafkaStreams} object created in the application
	 */
	void registerKafkaStreams(KafkaStreams kafkaStreams) {
		this.kafkaStreams.add(kafkaStreams);
	}
}

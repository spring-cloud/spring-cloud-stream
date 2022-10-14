/*
 * Copyright 2018-2022 the original author or authors.
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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.TopologyMetadata;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

/**
 * A facade to access topology info for a Kafka Streams application in a version agnostic
 * manner.
 * <p>Before kafka-streams 3.1 the topology exists at 'KafkaStreams.internalTopologyBuilder'.
 * Starting in kafka-streams 3.1 the topology exists at 'KafkaStreams.topologyMetadata'.
 *
 * @author Chris Bono
 * @since 3.2.6
 */
class KafkaStreamsVersionAgnosticTopologyInfoFacade {

	private final LogAccessor logger = new LogAccessor(KafkaStreamsVersionAgnosticTopologyInfoFacade.class);

	@Nullable
	private Field topologyInfoField;

	@Nullable
	private Method sourceTopicsForStoreMethod;

	KafkaStreamsVersionAgnosticTopologyInfoFacade() {

		// First look for KafkaStreams.internalTopologyBuilder (exists in kafka-streams <= 3.0)
		Field internalTopologyBuilderField = ReflectionUtils.findField(KafkaStreams.class, "internalTopologyBuilder");
		if (internalTopologyBuilderField != null) {
			internalTopologyBuilderField.setAccessible(true);
			this.topologyInfoField = internalTopologyBuilderField;
			this.sourceTopicsForStoreMethod = ReflectionUtils.findMethod(InternalTopologyBuilder.class, "sourceTopicsForStore", String.class);
			logger.info("Using KafkaStreams.internalTopologyBuilder.sourceTopicsForStore for kafka-streams <= 3.0");
		}

		// Otherwise look for KafkaStreams.topologyMetadata (exists in kafka-streams >= 3.1)
		Field topologyMetadataField = ReflectionUtils.findField(KafkaStreams.class, "topologyMetadata");
		if (topologyMetadataField != null) {
			topologyMetadataField.setAccessible(true);
			this.topologyInfoField = topologyMetadataField;
			this.sourceTopicsForStoreMethod = ReflectionUtils.findMethod(TopologyMetadata.class, "sourceTopicsForStore", String.class);
			logger.info("Using KafkaStreams.topologyMetadata.sourceTopicsForStore for kafka-streams >= 3.1");
		}

		if (this.sourceTopicsForStoreMethod == null) {
			logger.warn("Could not find 'topologyMetadata.sourceTopicsForStore' or 'internalTopologyBuilder.sourceTopicsForStore' " +
					"from KafkaStreams class - will be unable to reason about state stores.");
		}

	}

	/**
	 * Determines if a state store is actually available to a KafkaStreams instance by
	 * querying the topology info source topics for the requested store.
	 *
	 * @param kafkaStreams the streams app
	 * @param storeName the name of the state store
	 * @return {@code true} if state store is available or {@code false} if the state store is
	 * 	not available or there was a problem reflecting on the topology info
	 */
	boolean streamsAppActuallyHasStore(KafkaStreams kafkaStreams, String storeName) {
		if (this.sourceTopicsForStoreMethod == null) {
			logger.warn("Unable to reason about state store because sourceTopicsForStore method was not found - returning false");
			return false;
		}
		try {
			Object topologyInfo = ReflectionUtils.getField(this.topologyInfoField, kafkaStreams);
			if (topologyInfo != null) {
				logger.warn("Unable to reason about state store because topologyInfo field was null - returning false");
				return false;
			}
			Collection<String> sourceTopicsForStore = (Collection<String>)
					ReflectionUtils.invokeMethod(this.sourceTopicsForStoreMethod, topologyInfo, storeName);
			return !CollectionUtils.isEmpty(sourceTopicsForStore);
		}
		catch (Exception ex) {
			logger.error(ex, () -> "Unable to reason about state store due to error: " + ex.getMessage() + " - returning false");
		}
		return false;
	}
}

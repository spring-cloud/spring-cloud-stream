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

/**
 * An internal registry for holding {@KafkaStreams} objects maintained through
 * {@link StreamsBuilderFactoryManager}.
 *
 * @author Soby Chacko
 */
class KafkaStreamsRegistry {

	private final Set<KafkaStreams> kafkaStreams = new HashSet<>();

	Set<KafkaStreams> getKafkaStreams() {
		return this.kafkaStreams;
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

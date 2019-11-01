/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.KafkaStreams;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * An internal registry for holding {@KafkaStreams} objects maintained through
 * {@link StreamsBuilderFactoryManager}.
 *
 * @author Soby Chacko
 */
class KafkaStreamsRegistry {

	private Map<KafkaStreams, StreamsBuilderFactoryBean> streamsBuilderFactoryBeanMap = new HashMap<>();

	private final Set<KafkaStreams> kafkaStreams = new HashSet<>();

	Set<KafkaStreams> getKafkaStreams() {
		return this.kafkaStreams;
	}

	/**
	 * Register the {@link KafkaStreams} object created in the application.
	 * @param streamsBuilderFactoryBean {@link StreamsBuilderFactoryBean}
	 */
	void registerKafkaStreams(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		final KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
		this.kafkaStreams.add(kafkaStreams);
		this.streamsBuilderFactoryBeanMap.put(kafkaStreams, streamsBuilderFactoryBean);
	}

	/**
	 *
	 * @param kafkaStreams {@link KafkaStreams} object
	 * @return Corresponding {@link StreamsBuilderFactoryBean}.
	 */
	StreamsBuilderFactoryBean streamBuilderFactoryBean(KafkaStreams kafkaStreams) {
		return this.streamsBuilderFactoryBeanMap.get(kafkaStreams);
	}

}

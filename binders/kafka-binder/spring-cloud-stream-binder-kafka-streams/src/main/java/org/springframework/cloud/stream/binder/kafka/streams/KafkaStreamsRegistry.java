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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * An internal registry for holding {@link KafkaStreams} objects maintained through
 * {@link StreamsBuilderFactoryManager}.
 *
 * @author Soby Chacko
 */
public class KafkaStreamsRegistry {

	private final Map<KafkaStreams, StreamsBuilderFactoryBean> streamsBuilderFactoryBeanMap = new ConcurrentHashMap<>();

	private final Set<KafkaStreams> kafkaStreams = ConcurrentHashMap.newKeySet();

	Set<KafkaStreams> getKafkaStreams() {
		Set<KafkaStreams> currentlyRunningKafkaStreams = new HashSet<>();
		for (KafkaStreams ks : this.kafkaStreams) {
			final StreamsBuilderFactoryBean streamsBuilderFactoryBean = streamsBuilderFactoryBeanMap.get(ks);
			if (streamsBuilderFactoryBean.isRunning()) {
				currentlyRunningKafkaStreams.add(ks);
			}
		}
		return currentlyRunningKafkaStreams;
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

	void unregisterKafkaStreams(KafkaStreams kafkaStreams) {
		this.kafkaStreams.remove(kafkaStreams);
		this.streamsBuilderFactoryBeanMap.remove(kafkaStreams);
	}

	/**
	 *
	 * @param kafkaStreams {@link KafkaStreams} object
	 * @return Corresponding {@link StreamsBuilderFactoryBean}.
	 */
	StreamsBuilderFactoryBean streamBuilderFactoryBean(KafkaStreams kafkaStreams) {
		return this.streamsBuilderFactoryBeanMap.get(kafkaStreams);
	}

	public StreamsBuilderFactoryBean streamsBuilderFactoryBean(String applicationId) {
		final Optional<StreamsBuilderFactoryBean> first = this.streamsBuilderFactoryBeanMap.values()
				.stream()
				.filter(streamsBuilderFactoryBean -> streamsBuilderFactoryBean.isRunning() && Objects.requireNonNull(streamsBuilderFactoryBean
						.getStreamsConfiguration()).getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
						.equals(applicationId))
				.findFirst();
		return first.orElse(null);
	}

	public List<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans() {
		return new ArrayList<>(this.streamsBuilderFactoryBeanMap.values());
	}

}

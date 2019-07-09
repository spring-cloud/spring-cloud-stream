/*
 * Copyright 2017-2019 the original author or authors.
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

import org.apache.kafka.streams.kstream.TimeWindows;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsApplicationSupportProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Application support configuration for Kafka Streams binder.
 *
 * @deprecated Features provided on this class can be directly configured in the application itself using Kafka Streams.
 * @author Soby Chacko
 */
@Configuration
@EnableConfigurationProperties(KafkaStreamsApplicationSupportProperties.class)
@Deprecated
public class KafkaStreamsApplicationSupportAutoConfiguration {

	@Bean
	@ConditionalOnProperty("spring.cloud.stream.kafka.streams.timeWindow.length")
	public TimeWindows configuredTimeWindow(
			KafkaStreamsApplicationSupportProperties processorProperties) {
		return processorProperties.getTimeWindow().getAdvanceBy() > 0
				? TimeWindows.of(processorProperties.getTimeWindow().getLength())
						.advanceBy(processorProperties.getTimeWindow().getAdvanceBy())
				: TimeWindows.of(processorProperties.getTimeWindow().getLength());
	}

}

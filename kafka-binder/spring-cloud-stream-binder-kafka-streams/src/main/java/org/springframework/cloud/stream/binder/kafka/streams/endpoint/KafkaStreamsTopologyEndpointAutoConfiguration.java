/*
 * Copyright 2020-2020 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.endpoint;

import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsBinderSupportAutoConfiguration;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Soby Chacko
 * @since 3.0.4
 */
@Configuration
@ConditionalOnClass(name = {
		"org.springframework.boot.actuate.endpoint.annotation.Endpoint" })
@AutoConfigureAfter({EndpointAutoConfiguration.class, KafkaStreamsBinderSupportAutoConfiguration.class})
public class KafkaStreamsTopologyEndpointAutoConfiguration {

	@Bean
	@ConditionalOnAvailableEndpoint
	public KafkaStreamsTopologyEndpoint topologyEndpoint(KafkaStreamsRegistry kafkaStreamsRegistry) {
		return new KafkaStreamsTopologyEndpoint(kafkaStreamsRegistry);
	}
}

/*
 * Copyright 2015-2020 the original author or authors.
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

package org.springframework.cloud.bus.rsocket;

import io.rsocket.RSocket;
import io.rsocket.routing.client.spring.RoutingClientProperties;
import io.rsocket.routing.client.spring.RoutingRSocketRequester;

import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.bus.BusAutoConfiguration;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.BusRefreshAutoConfiguration;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.PathServiceMatcherAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Spencer Gibb
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBusEnabled
@EnableConfigurationProperties(BusRSocketProperties.class)
@ConditionalOnClass({ RSocket.class, RoutingRSocketRequester.class })
@AutoConfigureBefore({ BusAutoConfiguration.class, BusRefreshAutoConfiguration.class,
		PathServiceMatcherAutoConfiguration.class })
public class BusRSocketAutoConfiguration {

	@Bean
	public RoutingClientDestinationFactory routingClientDestinationFactory(BusRSocketProperties properties) {
		return new RoutingClientDestinationFactory(properties);
	}

	@Bean
	public RSocketRequesterBusBridge rSocketRequesterBusBridge(RoutingRSocketRequester requester) {
		return new RSocketRequesterBusBridge(requester);
	}

	@Bean
	public RSocketServiceMatcher rSocketServiceMatcher(BusProperties properties,
			RoutingClientProperties routingClientProperties) {
		return new RSocketServiceMatcher(properties.getId(), routingClientProperties);
	}

}

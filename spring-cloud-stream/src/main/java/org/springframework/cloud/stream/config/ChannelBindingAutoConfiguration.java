/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.OrderedHealthAggregator;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.cloud.stream.binding.ChannelBindingService;
import org.springframework.cloud.stream.endpoint.ChannelsEndpoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.MessageChannel;

/**
 * Configuration class with some useful beans for {@link MessageChannel} binding and
 * general Spring Integration infrastructure.
 *
 * @author Dave Syer
 * @author Marius Bogoevici
 */
@Configuration
@ConditionalOnBean(ChannelBindingService.class)
@EnableConfigurationProperties(DefaultPollerProperties.class)
@AutoConfigureBefore(EndpointAutoConfiguration.class)
public class ChannelBindingAutoConfiguration {

	@Autowired
	private DefaultPollerProperties poller;

	@Autowired(required = false)
	private List<Bindable> adapters;

	@Bean(name = PollerMetadata.DEFAULT_POLLER)
	@ConditionalOnMissingBean(PollerMetadata.class)
	public PollerMetadata defaultPoller() {
		return this.poller.getPollerMetadata();
	}

	@Bean
	public ChannelsEndpoint channelsEndpoint(ChannelBindingServiceProperties properties) {
		return new ChannelsEndpoint(adapters, properties);
	}

	@Bean
	@ConditionalOnEnabledHealthIndicator("binders")
	@ConditionalOnMissingBean(name = "bindersHealthIndicator")
	public CompositeHealthIndicator bindersHealthIndicator() {
		return new CompositeHealthIndicator(new OrderedHealthAggregator());
	}
}

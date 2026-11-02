/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.cloud.bus.jgroups;

import java.util.function.Consumer;

import org.jgroups.JChannel;

import tools.jackson.databind.ObjectMapper;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.bus.BusAutoConfiguration;
import org.springframework.cloud.bus.BusBridge;
import org.springframework.cloud.bus.BusStreamAutoConfiguration;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(before = { BusStreamAutoConfiguration.class, BusAutoConfiguration.class })
@ConditionalOnBusEnabled
@ConditionalOnClass({ JChannel.class, ObjectMapper.class })
@EnableConfigurationProperties(JGroupsBusProperties.class)
public class JGroupsBusAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	JGroupsChannelFactory jGroupsChannelFactory() {
		return new JGroupsChannelFactory();
	}

	@Bean
	@ConditionalOnMissingBean(BusBridge.class)
	JGroupsBusBridge jGroupsBusBridge(JGroupsBusProperties properties, ObjectMapper objectMapper,
			Consumer<RemoteApplicationEvent> eventConsumer, JGroupsChannelFactory channelFactory) {
		return new JGroupsBusBridge(properties, objectMapper, eventConsumer, channelFactory);
	}

}

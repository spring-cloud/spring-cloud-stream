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

package org.springframework.cloud.bus;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.autoconfigure.LifecycleMvcEndpointAutoConfiguration;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnBusEnabled
@ConditionalOnClass({ StreamBridge.class, BindingServiceConfiguration.class })
@EnableConfigurationProperties(BusProperties.class)
@AutoConfigureBefore({ BindingServiceConfiguration.class, BusAutoConfiguration.class })
// so stream bindings work properly
@AutoConfigureAfter({ LifecycleMvcEndpointAutoConfiguration.class, PathServiceMatcherAutoConfiguration.class })
public class BusStreamAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean(BusBridge.class)
	public StreamBusBridge streamBusBridge(StreamBridge streamBridge, BusProperties properties) {
		return new StreamBusBridge(streamBridge, properties);
	}

}

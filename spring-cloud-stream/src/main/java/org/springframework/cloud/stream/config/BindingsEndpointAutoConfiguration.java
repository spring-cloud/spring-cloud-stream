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

package org.springframework.cloud.stream.config;

import java.util.List;

import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.InputBindingLifecycle;
import org.springframework.cloud.stream.binding.OutputBindingLifecycle;
import org.springframework.cloud.stream.endpoint.BindingsEndpoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Oleg Zhurakousky
 * @since 2.0
 */
@Configuration
@ConditionalOnClass(name = {
		"org.springframework.boot.actuate.endpoint.annotation.Endpoint" })
@ConditionalOnBean(BindingService.class)
@AutoConfigureAfter(EndpointAutoConfiguration.class)
public class BindingsEndpointAutoConfiguration {

	@Bean
	public BindingsEndpoint bindingsEndpoint(List<InputBindingLifecycle> inputBindings,
			List<OutputBindingLifecycle> outputBindings) {
		return new BindingsEndpoint(inputBindings, outputBindings);
	}

}

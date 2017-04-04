/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.cloud.stream.endpoint.ChannelsEndpoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@Configuration
@ConditionalOnClass(name = "org.springframework.boot.actuate.endpoint.Endpoint")
@AutoConfigureAfter(EndpointAutoConfiguration.class)
public class ChannelsEndpointConfiguration {

	@Autowired(required = false)
	private List<Bindable> adapters;

	@Bean
	public ChannelsEndpoint channelsEndpoint(BindingServiceProperties properties) {
		return new ChannelsEndpoint(this.adapters, properties);
	}
}

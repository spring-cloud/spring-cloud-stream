/*
 * Copyright 2015-2023 the original author or authors.
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

package org.springframework.cloud.stream.endpoint;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.BeansException;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * An {@link Endpoint} that has the binding information on all the {@link Bindable}
 * message channels.
 *
 * @author Dave Syer
 * @author Ilayaperumal Gopinathan
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
@Endpoint(id = "channels")
public class ChannelsEndpoint implements ApplicationContextAware {

	private ApplicationContext applicationContext;

	private final BindingServiceProperties properties;

	public ChannelsEndpoint(BindingServiceProperties properties) {
		this.properties = properties;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@ReadOperation
	public Map<String, Object> channels() {
		Collection<Bindable> adapters = applicationContext.getBeansOfType(Bindable.class).values();
		ChannelsMetaData map = new ChannelsMetaData();
		Map<String, BindingProperties> inputs = map.getInputs();
		Map<String, BindingProperties> outputs = map.getOutputs();
		for (Bindable factory : adapters) {
			for (String name : factory.getInputs()) {
				inputs.put(name, this.properties.getBindingProperties(name));
			}
			for (String name : factory.getOutputs()) {
				outputs.put(name, this.properties.getBindingProperties(name));
			}
		}
		return new ObjectMapper().convertValue(map,
				new TypeReference<Map<String, Object>>() {
				});
	}

	/**
	 * Meta data for channels. Contains e.g. input and outputs.
	 */
	@JsonInclude(Include.NON_DEFAULT)
	public static class ChannelsMetaData {

		private Map<String, BindingProperties> inputs = new LinkedHashMap<>();

		private Map<String, BindingProperties> outputs = new LinkedHashMap<>();

		public Map<String, BindingProperties> getInputs() {
			return this.inputs;
		}

		public Map<String, BindingProperties> getOutputs() {
			return this.outputs;
		}

	}

}

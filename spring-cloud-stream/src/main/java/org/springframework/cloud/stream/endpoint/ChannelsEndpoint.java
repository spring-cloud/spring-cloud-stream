/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.endpoint;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.boot.actuate.endpoint.AbstractEndpoint;
import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;

/**
 * @author Dave Syer
 */
public class ChannelsEndpoint extends AbstractEndpoint<Map<String,Object>> {

	private List<Bindable> adapters;

	private ChannelBindingServiceProperties properties;

	public ChannelsEndpoint(List<Bindable> adapters,
			ChannelBindingServiceProperties properties) {
		super("channels");
		this.adapters = adapters;
		this.properties = properties;
	}

	@Override
	public Map<String,Object> invoke() {
		ChannelsMetaData map = new ChannelsMetaData();
		Map<String, BindingProperties> inputs = map.getInputs();
		Map<String, BindingProperties> outputs = map.getOutputs();
		for (Bindable factory : this.adapters) {
			Map<String, BindingProperties> bindings = this.properties.getBindings();
			for (String name : factory.getInputs()) {
				inputs.put(name, bindings.containsKey(name) ? bindings.get(name)
						: new BindingProperties());
			}
			for (String name : factory.getOutputs()) {
				outputs.put(name, bindings.containsKey(name) ? bindings.get(name)
						: new BindingProperties());
			}
		}
		return new ObjectMapper().convertValue(map, new TypeReference<Map<String,Object>>() {
		});
	}

	@JsonInclude(value = Include.NON_DEFAULT)
	public static class ChannelsMetaData {

		private Map<String, BindingProperties> inputs = new LinkedHashMap<>();

		private Map<String, BindingProperties> outputs = new LinkedHashMap<>();

		public Map<String, BindingProperties> getInputs() {
			return this.inputs;
		}

		public void setInputs(Map<String, BindingProperties> inputs) {
			this.inputs = inputs;
		}

		public Map<String, BindingProperties> getOutputs() {
			return this.outputs;
		}

		public void setOutputs(Map<String, BindingProperties> outputs) {
			this.outputs = outputs;
		}
	}

}

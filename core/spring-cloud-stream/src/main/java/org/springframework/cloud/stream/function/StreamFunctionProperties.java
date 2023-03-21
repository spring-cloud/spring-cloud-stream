/*
 * Copyright 2018-2023 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.util.StringUtils;

/**
 * @author Oleg Zhurakousky
 * @author Tolga Kavukcu
 * @author Soby Chacko
 * @since 2.1
 */
public class StreamFunctionProperties {

	/**
	 * Definition of functions to bind. If several functions need to be composed into one,
	 * use pipes (e.g., 'fooFunc|barFunc')
	 */
	private String definition;

	private BindingServiceProperties bindingServiceProperties;

	private Map<String, String> bindings = new HashMap<>();

	private boolean batchMode;

	private boolean composeTo;

	private boolean composeFrom;

	private Map<String, Boolean> reactive = new HashMap<>();

	boolean isComposeTo() {
		return composeTo;
	}

	boolean isComposeFrom() {
		return composeFrom;
	}

	public String getDefinition() {
		return this.definition;
	}

	public List<String> getOutputBindings(String functionName) {
		return this.filterBindings(functionName, "-out-");
	}

	public List<String> getInputBindings(String functionName) {
		return this.filterBindings(functionName, "-in-");
	}

	void setDefinition(String definition) {
		if (StringUtils.hasText(definition)) {
			this.composeFrom = definition.startsWith("|");
			this.composeTo = definition.endsWith("|");
			this.definition = this.composeFrom ? definition.substring(1)
					: (this.composeTo ? definition.substring(0, definition.length() - 1) : definition);
		}
	}

	BindingServiceProperties getBindingServiceProperties() {
		return this.bindingServiceProperties;
	}

	void setBindingServiceProperties(BindingServiceProperties bindingServiceProperties) {
		this.bindingServiceProperties = bindingServiceProperties;
	}

	public Map<String, String> getBindings() {
		return this.bindings;
	}

	public void setBindings(Map<String, String> bindings) {
		this.bindings = bindings;
	}

	boolean isBatchMode() {
		return this.batchMode;
	}

	void setBatchMode(boolean batchMode) {
		this.batchMode = batchMode;
	}

	private List<String> filterBindings(String functionName, String suffix) {
		List<String> list = bindings.keySet().stream()
				.filter(bKey -> bKey.contains(functionName + suffix))
				.sorted()
				.map(bKey -> bindings.get(bKey))
				.collect(Collectors.toList());
		return list;
	}

	Map<String, Boolean> getReactive() {
		return this.reactive;
	}

	void setReactive(Map<String, Boolean> reactive) {
		this.reactive = reactive;
	}

}

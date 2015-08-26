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

package org.springframework.cloud.stream.module.launcher;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.bind.PropertiesConfigurationFactory;
import org.springframework.core.env.PropertySources;

/**
 * A simple implementation of {@link ArgumentsNamespacingStrategy} that simply prefixes each module
 * argument with '{@literal <module key>.}'.
 *
 * <p>As an example, this is how one could specify the {@literal format} argument to the canonical time source:
 * <pre>
 *     org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT.format=hh:mm:ss
 * </pre>
 * </p>
 *
 * @author Eric Bottard
 */
public class DefaultArgumentsNamespacingStrategy implements ArgumentsNamespacingStrategy {

	@Override
	public Map<String, String> unqualify(String module, PropertySources propertySources) {
		PropertiesConfigurationFactory<Map<String, String>> propertiesFactory = new PropertiesConfigurationFactory<Map<String, String>>(new HashMap<String, String>());
		propertiesFactory.setPropertySources(propertySources);
		propertiesFactory.setTargetName(module);
		try {
			return propertiesFactory.getObject();
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Exception while trying to unqualify arguments for module '" + module + "'", e);
		}
	}

	@Override
	public Map<String, String> qualify(String module, Map<String, String> arguments) {
		Map<String, String> map = new HashMap<>(arguments.size());
		for (Map.Entry<String, String> kv : arguments.entrySet()) {
			map.put(String.format("%s.%s", module, kv.getKey()), kv.getValue());
		}
		return map;
	}
}

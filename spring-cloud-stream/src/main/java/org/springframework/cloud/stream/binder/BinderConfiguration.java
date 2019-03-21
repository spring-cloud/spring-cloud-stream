/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Map;
import java.util.Properties;

/**
 * Configuration for a binder instance, associating a {@link BinderType} with its
 * configuration {@link Properties}. An application may contain multiple
 * {@link BinderConfiguration}s per {@link BinderType}, when connecting to multiple
 * systems of the same type.
 *
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 */
public class BinderConfiguration {

	private final String binderType;

	private final Map<String, Object> properties;

	private final boolean inheritEnvironment;

	private final boolean defaultCandidate;

	/**
	 * @param binderType the binder type used by this configuration
	 * @param properties the properties for setting up the binder
	 * @param inheritEnvironment whether the binder should inherit the environment of the
	 * application
	 * @param defaultCandidate whether the binder should be considered as a candidate when
	 * determining a default
	 */
	public BinderConfiguration(String binderType, Map<String, Object> properties,
			boolean inheritEnvironment, boolean defaultCandidate) {
		this.binderType = binderType;
		this.properties = properties;
		this.inheritEnvironment = inheritEnvironment;
		this.defaultCandidate = defaultCandidate;
	}

	public String getBinderType() {
		return this.binderType;
	}

	public Map<String, Object> getProperties() {
		return this.properties;
	}

	public boolean isInheritEnvironment() {
		return this.inheritEnvironment;
	}

	public boolean isDefaultCandidate() {
		return this.defaultCandidate;
	}

}

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

package org.springframework.cloud.stream.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Contains the properties of a binder.
 *
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 */
public class BinderProperties {

	/**
	 * The binder type. It typically references one of the binders found on the classpath,
	 * in particular a key in a META-INF/spring.binders file. By default, it has the same
	 * value as the configuration name.
	 */
	private String type;

	/**
	 * Root for a set of properties that can be used to customize the environment of the
	 * binder.
	 */
	private Map<String, Object> environment = new HashMap<>();

	/**
	 * Whether the configuration will inherit the environment of the application itself.
	 * Default: true
	 */
	private boolean inheritEnvironment = true;

	/**
	 * Whether the binder configuration is a candidate for being considered a default
	 * binder, or can be used only when explicitly referenced. Defaulys: true
	 */
	private boolean defaultCandidate = true;

	public String getType() {
		return this.type;
	}

	public void setType(String name) {
		this.type = name;
	}

	public Map<String, Object> getEnvironment() {
		return this.environment;
	}

	/**
	 * @deprecated in 2.0.0 in preference to {@link #setEnvironment(Map)}
	 * @param environment properties to which stream props will be added
	 */
	@Deprecated
	public void setEnvironment(Properties environment) {
		this.environment.clear();
		this.environment.putAll(environment.entrySet().stream().collect(
				Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue())));
	}

	public void setEnvironment(Map<String, Object> environment) {
		this.environment = environment;
	}

	public boolean isInheritEnvironment() {
		return this.inheritEnvironment;
	}

	public void setInheritEnvironment(boolean inheritEnvironment) {
		this.inheritEnvironment = inheritEnvironment;
	}

	public boolean isDefaultCandidate() {
		return this.defaultCandidate;
	}

	public void setDefaultCandidate(boolean defaultCandidate) {
		this.defaultCandidate = defaultCandidate;
	}

}

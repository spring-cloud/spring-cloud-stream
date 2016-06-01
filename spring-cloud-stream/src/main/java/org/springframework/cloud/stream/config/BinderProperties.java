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

package org.springframework.cloud.stream.config;

import java.util.Properties;

/**
 * Contains the properties of a binder.
 *
 * @author Marius Bogoevici
 */
public class BinderProperties {

	private String type;

	private Properties environment = new Properties();

	private boolean inheritEnvironment = true;

	private boolean defaultCandidate = true;

	public String getType() {
		return type;
	}

	public void setType(String name) {
		this.type = name;
	}

	public Properties getEnvironment() {
		return environment;
	}

	public void setEnvironment(Properties environment) {
		this.environment = environment;
	}

	public boolean isInheritEnvironment() {
		return inheritEnvironment;
	}

	public void setInheritEnvironment(boolean inheritEnvironment) {
		this.inheritEnvironment = inheritEnvironment;
	}

	public boolean isDefaultCandidate() {
		return defaultCandidate;
	}

	public void setDefaultCandidate(boolean defaultCandidate) {
		this.defaultCandidate = defaultCandidate;
	}
}

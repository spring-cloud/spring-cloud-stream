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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.hibernate.validator.constraints.NotEmpty;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySources;

/**
 * Configuration properties for {@link ModuleLauncher}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
@ConfigurationProperties
public class ModuleLauncherProperties {

	/**
	 * Array of coordinates for modules that need to be launched.
	 */
	private String[] modules;

	/**
	 * Map of arguments, keyed by the 0-based index in the {@kink #modules array}.
	 */
	private Map<Integer, Map<String, String>> arguments = new HashMap<>();

	public void setModules(String[] modules) {
		this.modules = modules;
	}

	@NotEmpty(message = "A list of modules must be specified.")
	public String[] getModules() {
		return modules;
	}

	public void setArguments(Map<Integer, Map<String, String>> arguments) {
		this.arguments = arguments;
	}

	public Map<Integer, Map<String, String>> getArguments() {
		return arguments;
	}

	public List<ModuleLaunchRequest> asModuleLaunchRequests() {
		List<ModuleLaunchRequest> requests = new ArrayList<>();
		for (int i = 0; i < modules.length; i++) {
			ModuleLaunchRequest moduleLaunchRequest = new ModuleLaunchRequest(modules[i]);
			moduleLaunchRequest.setArguments(arguments.get(i));
			requests.add(moduleLaunchRequest);
		}
		return requests;
	}
}

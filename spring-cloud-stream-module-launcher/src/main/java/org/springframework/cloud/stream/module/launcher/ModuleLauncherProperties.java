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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.hibernate.validator.constraints.NotEmpty;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.bind.PropertiesConfigurationFactory;
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
	 * Array of modules that need to be launched.
	 */
	private String[] modules;

	private List<ModuleWithArguments> modulesWithArguments = new ArrayList<>();

	private PropertySources propertySources;

	@Autowired
	private ArgumentsNamespacingStrategy argumentsNamespacingStrategy;

	@Autowired
	public void setEnvironment(Environment environment) {
		if (environment instanceof ConfigurableEnvironment) {
			ConfigurableEnvironment configurableEnvironment = (ConfigurableEnvironment) environment;
			propertySources = configurableEnvironment.getPropertySources();
		}
	}

	public void setModules(String[] modules) {
		this.modules = modules;
	}

	@NotEmpty(message = "A list of modules must be specified.")
	public String[] getModules() {
		return modules;
	}

	@PostConstruct
	public void deduceModuleArguments() throws Exception {
		for (String module : modules) {
			ModuleWithArguments moduleWithArguments = new ModuleWithArguments(module);
			moduleWithArguments.setArguments(argumentsNamespacingStrategy.unqualify(module, this.propertySources));
			modulesWithArguments.add(moduleWithArguments);
		}
	}

	public List<ModuleWithArguments> modulesWithArguments() {
		return modulesWithArguments;
	}
}

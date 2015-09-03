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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Spring boot {@link ApplicationRunner} that triggers {@link ModuleLauncher} to launch the modules.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Eric Bottard
 */
@Component
@EnableConfigurationProperties(ModuleLauncherProperties.class)
public class ModuleLauncherRunner implements CommandLineRunner {

	private final static Log log = LogFactory.getLog(ModuleLauncherRunner.class);

	@Autowired
	private ModuleLauncherProperties moduleLauncherProperties;

	@Autowired
	private ModuleLauncher moduleLauncher;

	@Override
	public void run(String... args) throws Exception {
		List<ModuleLaunchRequest> launchRequests = generateModuleLaunchRequests();
		if (log.isInfoEnabled()) {
			StringBuilder sb = new StringBuilder("Launching\n");
			for (ModuleLaunchRequest moduleLaunchRequest : launchRequests) {
				sb.append('\t').append(moduleLaunchRequest).append('\n');
			}
			log.info(sb.toString());
		}
		this.moduleLauncher.launch(launchRequests,
				moduleLauncherProperties.isAggregate(),
				moduleLauncherProperties.isAggregate() ? extractAggregateProperties(args) : new String[0]);
	}

	private List<ModuleLaunchRequest> generateModuleLaunchRequests() {
		List<ModuleLaunchRequest> requests = new ArrayList<>();
		String[] modules = this.moduleLauncherProperties.getModules();
		Map<ModuleOptionKey, String> arguments = this.moduleLauncherProperties.getArgs();
		Map<Integer, Map<String,String>> properties = new HashMap<>();
		for (int i = 0; i < modules.length; i++) {
			Map<String, String> options = new HashMap<>();
			for (Map.Entry<ModuleOptionKey, String> value: arguments.entrySet()) {
				if (value.getKey().getIndex() == i) {
					options.put(value.getKey().getOption(), value.getValue());
				}
			}
			properties.put(i, options);
			ModuleLaunchRequest moduleLaunchRequest = new ModuleLaunchRequest(modules[i], properties.get(i));
			requests.add(moduleLaunchRequest);
		}
		return requests;
	}

	private String[] extractAggregateProperties(String[] args) {
		List<String> filteredProperties = new ArrayList<>();
		for (String arg : args) {
			if (arg.startsWith("--spring.")) {
				filteredProperties.add(arg);
			}
		}
		return filteredProperties.toArray(new String[filteredProperties.size()]);
	}
}

/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.module.launcher;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 */
@Component
@ConfigurationProperties
public class ModuleLauncherRunner implements ApplicationRunner,InitializingBean {

	@Autowired
	private ModuleLauncher moduleLauncher;

	private String modules;

	public void setModules(String modules) {
		this.modules = modules;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (!StringUtils.hasText(modules)) {
			System.err.println("Either the 'modules' system property or 'MODULES' environment variable is required.");
			System.exit(1);
		}
	}

	@Override
	public void run(ApplicationArguments applicationArguments) throws Exception {
		moduleLauncher.launch(modules.split(","), applicationArguments.getSourceArgs());
	}
}

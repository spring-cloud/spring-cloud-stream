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

import java.io.File;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.module.resolver.AetherModuleResolver;
import org.springframework.cloud.stream.module.resolver.ModuleResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Marius Bogoevici
 */
@Configuration
@ConfigurationProperties
public class ModuleLauncherConfiguration {

	public static final String DEFAULT_LOCAL_REPO =
			System.getProperty("user.home") + File.separator  + ".m2" + File.separator +  "repository";

	public static final String DEFAULT_REMOTE_REPO = "https://repo.spring.io/libs-snapshot";

	private File localRepository = new File(DEFAULT_LOCAL_REPO);

	private String remoteRepository = DEFAULT_REMOTE_REPO;

	public void setLocalRepository(File localRepository) {
		this.localRepository = localRepository;
	}

	public void setRemoteRepository(String remoteRepository) {
		this.remoteRepository = remoteRepository;
	}

	/**
	 * Sets up the default Aether-based module resolver, unless overridden
	 */
	@Bean
	@ConditionalOnMissingBean(ModuleResolver.class)
	public ModuleResolver moduleResolver() {
		return new AetherModuleResolver(localRepository, Collections.singletonMap("remoteRepository", remoteRepository));
	}

	@Bean
	public ModuleLauncher moduleLauncher(ModuleResolver moduleResolver) {
		return new ModuleLauncher(moduleResolver);
	}

}

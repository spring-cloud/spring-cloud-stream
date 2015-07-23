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

package org.springframework.cloud.stream.module.registry;

import java.nio.file.Files;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;

/**
 * Will create a {@link SynchronizingModuleRegistry} for hosting custom modules if necessary, depending on the
 * protocol scheme (using a synchronizing registry for everything but {@code file://} protocol).
 * @author Eric Bottard
 * @since 1.2
 */
public class CustomModuleRegistryFactoryBean implements FactoryBean<WritableModuleRegistry>, EnvironmentAware, 
		InitializingBean {


	private static final Logger logger = LoggerFactory.getLogger(CustomModuleRegistryFactoryBean.class);

	private static final Pattern NO_SYNCHRONIZATION_PATTERN = Pattern.compile("^file:.*");

	public static final String LOCAL_ROOT = "spring-cloud-stream-custom-modules";

	private WritableModuleRegistry registry;

	private final String root;

	private ConfigurableEnvironment environment;


	public CustomModuleRegistryFactoryBean(String root) {
		this.root = root;
	}

	@Override
	public WritableModuleRegistry getObject() throws Exception {
		return registry;
	}

	@Override
	public Class<WritableModuleRegistry> getObjectType() {
		return WritableModuleRegistry.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = (ConfigurableEnvironment) environment;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Matcher matcher = NO_SYNCHRONIZATION_PATTERN.matcher(root);
		if (matcher.matches()) {
			registry = new WritableResourceModuleRegistry(root);
			((WritableResourceModuleRegistry) registry).afterPropertiesSet();
			logger.info("Custom modules will be written directly to {}", root);
		}
		else {
			String localRoot = "file:" + Files.createTempDirectory(LOCAL_ROOT);
			WritableResourceModuleRegistry local = new WritableResourceModuleRegistry(localRoot);
			local.afterPropertiesSet();

			WritableResourceModuleRegistry remote = new WritableResourceModuleRegistry(root);
			remote.setEnvironment(environment);
			remote.afterPropertiesSet();

			registry = new SynchronizingModuleRegistry(remote, local);
			logger.info("Custom modules will be written at {} and kept in synch locally at {}", root, localRoot);
		}
	}
}

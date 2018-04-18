/*
 * Copyright 2015-2018 the original author or authors.
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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.binder.BinderConfiguration;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binder.DefaultBinderTypeRegistry;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
@Configuration
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class BinderFactoryConfiguration {

	protected final Log logger = LogFactory.getLog(getClass());

	private static final String SPRING_CLOUD_STREAM_INTERNAL_PREFIX = "spring.cloud.stream.internal";

	private static final String SELF_CONTAINED_APP_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX
			+ ".selfContained";

	@Value("${" + SELF_CONTAINED_APP_PROPERTY_NAME + ":}")
	private String selfContained;

	@Autowired(required = false)
	private Collection<DefaultBinderFactory.Listener> binderFactoryListeners;

	static Collection<BinderType> parseBinderConfigurations(ClassLoader classLoader, Resource resource)
			throws IOException, ClassNotFoundException {
		Properties properties = PropertiesLoaderUtils.loadProperties(resource);
		Collection<BinderType> parsedBinderConfigurations = new ArrayList<>();
		for (Map.Entry<?, ?> entry : properties.entrySet()) {
			String binderType = (String) entry.getKey();
			String[] binderConfigurationClassNames = StringUtils
					.commaDelimitedListToStringArray((String) entry.getValue());
			Class<?>[] binderConfigurationClasses = new Class[binderConfigurationClassNames.length];
			int i = 0;
			for (String binderConfigurationClassName : binderConfigurationClassNames) {
				binderConfigurationClasses[i++] = ClassUtils.forName(binderConfigurationClassName, classLoader);
			}
			parsedBinderConfigurations.add(new BinderType(binderType, binderConfigurationClasses));
		}
		return parsedBinderConfigurations;
	}

	@Bean
	@ConditionalOnMissingBean(BinderFactory.class)
	public DefaultBinderFactory binderFactory(BinderTypeRegistry binderTypeRegistry,
			BindingServiceProperties bindingServiceProperties) {
		DefaultBinderFactory binderFactory = new DefaultBinderFactory(
				getBinderConfigurations(binderTypeRegistry, bindingServiceProperties), binderTypeRegistry);
		binderFactory.setDefaultBinder(bindingServiceProperties.getDefaultBinder());
		binderFactory.setListeners(binderFactoryListeners);
		return binderFactory;
	}

	private Map<String, BinderConfiguration> getBinderConfigurations(BinderTypeRegistry binderTypeRegistry,
			BindingServiceProperties bindingServiceProperties) {
		Map<String, BinderConfiguration> binderConfigurations = new HashMap<>();
		Map<String, BinderProperties> declaredBinders = bindingServiceProperties.getBinders();
		boolean defaultCandidatesExist = false;
		Iterator<Map.Entry<String, BinderProperties>> binderPropertiesIterator = declaredBinders.entrySet().iterator();
		while (!defaultCandidatesExist && binderPropertiesIterator.hasNext()) {
			defaultCandidatesExist = binderPropertiesIterator.next().getValue().isDefaultCandidate();
		}
		List<String> existingBinderConfigurations = new ArrayList<>();
		for (Map.Entry<String, BinderProperties> binderEntry : declaredBinders.entrySet()) {
			BinderProperties binderProperties = binderEntry.getValue();
			if (binderTypeRegistry.get(binderEntry.getKey()) != null) {
				binderConfigurations.put(binderEntry.getKey(),
						new BinderConfiguration(binderEntry.getKey(),
								binderProperties.getEnvironment(), binderProperties.isInheritEnvironment(),
								binderProperties.isDefaultCandidate()));
				existingBinderConfigurations.add(binderEntry.getKey());
			}
			else {
				Assert.hasText(binderProperties.getType(),
						"No 'type' property present for custom binder " + binderEntry.getKey());
				binderConfigurations.put(binderEntry.getKey(),
						new BinderConfiguration(binderProperties.getType(), binderProperties.getEnvironment(),
								binderProperties.isInheritEnvironment(), binderProperties.isDefaultCandidate()));
				existingBinderConfigurations.add(binderEntry.getKey());
			}
		}
		for (Map.Entry<String, BinderConfiguration> configurationEntry : binderConfigurations.entrySet()) {
			if (configurationEntry.getValue().isDefaultCandidate()) {
				defaultCandidatesExist = true;
			}
		}
		if (!defaultCandidatesExist) {
			for (Map.Entry<String, BinderType> binderEntry : binderTypeRegistry.getAll().entrySet()) {
				if (!existingBinderConfigurations.contains(binderEntry.getKey())) {
					binderConfigurations.put(binderEntry.getKey(), new BinderConfiguration(binderEntry.getKey(),
							new HashMap<>(), true, true));
				}
			}
		}
		return binderConfigurations;
	}

	@Bean
	@ConditionalOnMissingBean(BinderTypeRegistry.class)
	public BinderTypeRegistry binderTypeRegistry(ConfigurableApplicationContext configurableApplicationContext) {
		Map<String, BinderType> binderTypes = new HashMap<>();
		ClassLoader classLoader = configurableApplicationContext.getClassLoader();
		// the above can never be null since it will default to ClassUtils.getDefaultClassLoader(..)
		try {
			Enumeration<URL> resources = classLoader.getResources("META-INF/spring.binders");
			if (!Boolean.valueOf(this.selfContained) && (resources == null || !resources.hasMoreElements())) {
				this.logger.debug("Failed to locate 'META-INF/spring.binders' resources on the classpath."
						+ " Assuming standard boot 'META-INF/spring.factories' configuration is used");
			}
			else {
				while (resources.hasMoreElements()) {
					URL url = resources.nextElement();
					UrlResource resource = new UrlResource(url);
					for (BinderType binderType : parseBinderConfigurations(classLoader, resource)) {
						binderTypes.put(binderType.getDefaultName(), binderType);
					}
				}
			}

		}
		catch (IOException | ClassNotFoundException e) {
			throw new BeanCreationException("Cannot create binder factory:", e);
		}
		return new DefaultBinderTypeRegistry(binderTypes);
	}
}

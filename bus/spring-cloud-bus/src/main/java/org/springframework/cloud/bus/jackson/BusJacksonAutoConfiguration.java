/*
 * Copyright 2012-2019 the original author or authors.
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

package org.springframework.cloud.bus.jackson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.cloud.bus.BusAutoConfiguration;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.endpoint.RefreshBusEndpoint;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.bus.event.UnknownRemoteApplicationEvent;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeTypeUtils;

/**
 * @author Spencer Gibb
 * @author Dave Syer
 * @author Donovan Muller
 * @author Stefan Pfeiffer
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBusEnabled
@ConditionalOnClass({ RefreshBusEndpoint.class, ObjectMapper.class })
@AutoConfigureBefore({ BusAutoConfiguration.class, JacksonAutoConfiguration.class })
public class BusJacksonAutoConfiguration {

	// needed in the case where @RemoteApplicationEventScan is not used
	// otherwise RemoteApplicationEventRegistrar will register the bean
	@Bean
	@ConditionalOnMissingBean(name = "busJsonConverter")
	@StreamMessageConverter
	public AbstractMessageConverter busJsonConverter(
			@Autowired(required = false) ObjectMapper objectMapper) {
		return new BusJacksonMessageConverter(objectMapper);
	}

}

class BusJacksonMessageConverter extends AbstractMessageConverter
		implements InitializingBean {

	private static final Log log = LogFactory.getLog(BusJacksonMessageConverter.class);

	private static final String DEFAULT_PACKAGE = ClassUtils
			.getPackageName(RemoteApplicationEvent.class);

	private final ObjectMapper mapper;

	private final boolean mapperCreated;

	private String[] packagesToScan = new String[] { DEFAULT_PACKAGE };

	private BusJacksonMessageConverter() {
		this(null);
	}

	@Autowired(required = false)
	BusJacksonMessageConverter(@Nullable ObjectMapper objectMapper) {
		super(MimeTypeUtils.APPLICATION_JSON);

		if (objectMapper != null) {
			this.mapper = objectMapper;
			this.mapperCreated = false;
		}
		else {
			this.mapper = new ObjectMapper();
			this.mapperCreated = true;
		}
	}

	/* for testing */ boolean isMapperCreated() {
		return this.mapperCreated;
	}

	/* for testing */ ObjectMapper getMapper() {
		return this.mapper;
	}

	public void setPackagesToScan(String[] packagesToScan) {
		List<String> packages = new ArrayList<>(Arrays.asList(packagesToScan));
		if (!packages.contains(DEFAULT_PACKAGE)) {
			packages.add(DEFAULT_PACKAGE);
		}
		this.packagesToScan = packages.toArray(new String[0]);
	}

	private Class<?>[] findSubTypes() {
		List<Class<?>> types = new ArrayList<>();
		if (this.packagesToScan != null) {
			for (String pkg : this.packagesToScan) {
				ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(
						false);
				provider.addIncludeFilter(
						new AssignableTypeFilter(RemoteApplicationEvent.class));

				Set<BeanDefinition> components = provider.findCandidateComponents(pkg);
				for (BeanDefinition component : components) {
					try {
						types.add(Class.forName(component.getBeanClassName()));
					}
					catch (ClassNotFoundException e) {
						throw new IllegalStateException(
								"Failed to scan classpath for remote event classes", e);
					}
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Found sub types: " + types);
		}
		return types.toArray(new Class<?>[0]);
	}

	@Override
	protected boolean supports(Class<?> aClass) {
		// This converter applies only to RemoteApplicationEvent and subclasses
		return RemoteApplicationEvent.class.isAssignableFrom(aClass);
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass,
			Object conversionHint) {
		Object result = null;
		try {
			Object payload = message.getPayload();

			if (payload instanceof byte[]) {
				try {
					result = this.mapper.readValue((byte[]) payload, targetClass);
				}
				catch (InvalidTypeIdException e) {
					return new UnknownRemoteApplicationEvent(new Object(), e.getTypeId(),
							(byte[]) payload);
				}
			}
			else if (payload instanceof String) {
				try {
					result = this.mapper.readValue((String) payload, targetClass);
				}
				catch (InvalidTypeIdException e) {
					return new UnknownRemoteApplicationEvent(new Object(), e.getTypeId(),
							((String) payload).getBytes());
				}
				// workaround for
				// https://github.com/spring-cloud/spring-cloud-stream/issues/1564
			}
			else if (payload instanceof RemoteApplicationEvent) {
				return payload;
			}
		}
		catch (Exception e) {
			this.logger.error(e.getMessage(), e);
			return null;
		}
		return result;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.mapper.registerModule(new SubtypeModule(findSubTypes()));
	}

}

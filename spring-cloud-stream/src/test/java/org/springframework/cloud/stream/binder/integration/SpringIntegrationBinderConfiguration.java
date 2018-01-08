/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DefaultBinderTypeRegistry;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.integration.config.EnableIntegration;

/**
 * {@link Binder} configuration backed by Spring Integration.
 *
 * Please see {@link SpringIntegrationChannelBinder} for more details.
 *
 * @author Oleg Zhurakousky
 *
 * @see SpringIntegrationChannelBinder
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@EnableIntegration
public class SpringIntegrationBinderConfiguration<T> {

	public static final String NAME = "integration";

	/**
	 * Utility operation to return an array of configuration classes
	 * defined in {@link EnableBinding} annotation.
	 * Typically used for tests that do not rely on creating an SCSt boot
	 * application annotated with {@link EnableBinding}, yet require
	 * full {@link Binder} configuration.
	 */
	public static Class<?>[] getCompleteConfiguration() {
		List<Class<?>> configClasses = new ArrayList<>();
		configClasses.add(SpringIntegrationBinderConfiguration.class);
		Import annotation = AnnotationUtils.getAnnotation(EnableBinding.class, Import.class);
		Map<String, Object> annotationAttributes = AnnotationUtils.getAnnotationAttributes(annotation);
		configClasses.addAll(Arrays.asList((Class<?>[])annotationAttributes.get("value")));
		return configClasses.toArray(new Class<?>[] {});
	}

	@Bean
	public BinderTypeRegistry binderTypeRegistry() {
		BinderType binderType = new BinderType(NAME, new Class[] {SpringIntegrationBinderConfiguration.class});
		BinderTypeRegistry btr = new DefaultBinderTypeRegistry(Collections.singletonMap(NAME, binderType));
		return btr;
	}

	@Bean
	public SourceDestination sourceDestination() {
		return new SourceDestination();
	}

	@Bean
	public TargetDestination targetDestination() {
		return new TargetDestination();
	}

	@SuppressWarnings("unchecked")
	@Bean
	public Binder<T, ? extends ConsumerProperties, ? extends ProducerProperties> springIntegrationChannelBinder(SpringIntegrationProvisioner provisioner) {
		return (Binder<T, ? extends ConsumerProperties, ? extends ProducerProperties>) new SpringIntegrationChannelBinder(provisioner);
	}

	@Bean
	public SpringIntegrationProvisioner springIntegrationProvisioner() {
		return new SpringIntegrationProvisioner();
	}

}

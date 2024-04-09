/*
 * Copyright 2017-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.config.EnableIntegration;

/**
 * {@link Binder} configuration backed by Spring Integration.
 *
 * Please see {@link TestChannelBinder} for more details.
 *
 * @param <T> binding type
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @author Soby Chacko
 * @see TestChannelBinder
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@Import(BinderFactoryAutoConfiguration.class)
@EnableIntegration
public class TestChannelBinderConfiguration<T> {

	/**
	 * The name of the test binder.
	 */
	public static final String NAME = "integration";

	/**
	 * Utility operation to return an array of configuration classes where function
	 * definitions are provided. Typically used for tests that do not rely on
	 * creating an SCSt boot application, yet require full {@link Binder} configuration.
	 *
	 * @param additionalConfigurationClasses config classes to be added to the default config
	 * @return an array of configuration classes
	 */
	public static Class<?>[] getCompleteConfiguration(
		Class<?>... additionalConfigurationClasses) {
		List<Class<?>> configClasses = new ArrayList<>();
		configClasses.add(TestChannelBinderConfiguration.class);
		configClasses.add(BindingServiceConfiguration.class);
		if (additionalConfigurationClasses != null) {
			configClasses.addAll(Arrays.asList(additionalConfigurationClasses));
		}
		return configClasses.toArray(new Class<?>[] {});
	}

	/**
	 * Create an {@link ApplicationContextRunner} with user configuration using {@link #getCompleteConfiguration}.
	 * @param additionalConfigurationClasses config classes to be added to the default
	 * config
	 * @return the ApplicationContextRunner
	 */
	public static ApplicationContextRunner applicationContextRunner(Class<?>... additionalConfigurationClasses) {
		return new ApplicationContextRunner()
			.withUserConfiguration(getCompleteConfiguration(additionalConfigurationClasses));
	}

	@Bean
	public InputDestination sourceDestination() {
		return new InputDestination();
	}

	@Bean
	public OutputDestination targetDestination() {
		return new OutputDestination();
	}

	@SuppressWarnings("unchecked")
	@Bean
	public Binder<T, ? extends ConsumerProperties, ? extends ProducerProperties> springIntegrationChannelBinder(
		TestChannelBinderProvisioner provisioner) {
		return (Binder<T, ? extends ConsumerProperties, ? extends ProducerProperties>) new TestChannelBinder(
			provisioner);
	}

	@Bean
	public TestChannelBinderProvisioner springIntegrationProvisioner() {
		return new TestChannelBinderProvisioner();
	}

}

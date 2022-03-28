/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.NamedContributor;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Ilayaperumal Gopinathan
 */
@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
@ConditionalOnEnabledHealthIndicator("binders")
@AutoConfigureBefore(EndpointAutoConfiguration.class)
@ConditionalOnBean(BinderFactory.class)
@AutoConfigureAfter(BindingServiceConfiguration.class)
@Configuration(proxyBeanMethods = false)
public class BindersHealthIndicatorAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public BindersHealthContributor bindersHealthContributor() {
		return new BindersHealthContributor();
	}

	@Bean
	public DefaultBinderFactory.Listener bindersHealthIndicatorListener(
			BindersHealthContributor bindersHealthContributor) {
		return new BindersHealthIndicatorListener(bindersHealthContributor);
	}

	/**
	 * A {@link DefaultBinderFactory.Listener} that provides {@link HealthIndicator}
	 * support.
	 */
	public static class BindersHealthIndicatorListener
			implements DefaultBinderFactory.Listener {

		private final BindersHealthContributor bindersHealthContributor;

		BindersHealthIndicatorListener(BindersHealthContributor bindersHealthContributor) {
			this.bindersHealthContributor = bindersHealthContributor;
		}

		@Override
		public void afterBinderContextInitialized(String binderConfigurationName,
				ConfigurableApplicationContext binderContext) {
			if (this.bindersHealthContributor != null) {
				this.bindersHealthContributor.add(binderConfigurationName,
						binderContext.getBeansOfType(HealthContributor.class));
			}
		}

	}

	/**
	 * {@link CompositeHealthContributor} that provides binder health contributions.
	 */
	public static class BindersHealthContributor implements CompositeHealthContributor {

		private static final HealthIndicator UNKNOWN = () -> Health.unknown().build();

		private Map<String, HealthContributor> contributors = new LinkedHashMap<>();

		void add(String binderConfigurationName, Map<String, HealthContributor> binderHealthContributors) {
			// if there are no health contributors in the child context, we just mark
			// the binder's health as unknown
			// this can happen due to the fact that configuration is inherited
			this.contributors.put(binderConfigurationName, getContributor(binderHealthContributors));
		}

		private HealthContributor getContributor(Map<String, HealthContributor> binderHealthContributors) {
			if (binderHealthContributors.isEmpty()) {
				return UNKNOWN;
			}
			if (binderHealthContributors.size() == 1) {
				return binderHealthContributors.values().iterator().next();
			}
			return CompositeHealthContributor.fromMap(binderHealthContributors);
		}

		@Override
		public HealthContributor getContributor(String name) {
			return contributors.get(name);
		}

		@Override
		public Iterator<NamedContributor<HealthContributor>> iterator() {
			return contributors.entrySet().stream()
					.map((entry) -> NamedContributor.of(entry.getKey(), entry.getValue())).iterator();
		}

	}

}

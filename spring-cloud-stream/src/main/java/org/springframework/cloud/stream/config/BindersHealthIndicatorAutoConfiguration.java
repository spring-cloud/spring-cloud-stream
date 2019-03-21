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

import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.DefaultHealthIndicatorRegistry;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.OrderedHealthAggregator;
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
@Configuration
public class BindersHealthIndicatorAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean(name = "bindersHealthIndicator")
	public CompositeHealthIndicator bindersHealthIndicator() {
		return new CompositeHealthIndicator(new OrderedHealthAggregator(),
				new DefaultHealthIndicatorRegistry());
	}

	@Bean
	public DefaultBinderFactory.Listener bindersHealthIndicatorListener(
			@Qualifier("bindersHealthIndicator") CompositeHealthIndicator compositeHealthIndicator) {
		return new BindersHealthIndicatorListener(compositeHealthIndicator);
	}

	/**
	 * A {@link DefaultBinderFactory.Listener} that provides {@link HealthIndicator}
	 * support.
	 *
	 * @author Ilayaperumal Gopinathan
	 */
	private static class BindersHealthIndicatorListener
			implements DefaultBinderFactory.Listener {

		private final CompositeHealthIndicator bindersHealthIndicator;

		BindersHealthIndicatorListener(CompositeHealthIndicator bindersHealthIndicator) {
			this.bindersHealthIndicator = bindersHealthIndicator;
		}

		@Override
		public void afterBinderContextInitialized(String binderConfigurationName,
				ConfigurableApplicationContext binderContext) {
			if (this.bindersHealthIndicator != null) {
				OrderedHealthAggregator healthAggregator = new OrderedHealthAggregator();
				Map<String, HealthIndicator> indicators = binderContext
						.getBeansOfType(HealthIndicator.class);
				// if there are no health indicators in the child context, we just mark
				// the binder's health as unknown
				// this can happen due to the fact that configuration is inherited
				HealthIndicator binderHealthIndicator = indicators.isEmpty()
						? new DefaultHealthIndicator()
						: new CompositeHealthIndicator(healthAggregator, indicators);
				this.bindersHealthIndicator.getRegistry()
						.register(binderConfigurationName, binderHealthIndicator);
			}
		}

		private static class DefaultHealthIndicator extends AbstractHealthIndicator {

			@Override
			protected void doHealthCheck(Health.Builder builder) throws Exception {
				builder.unknown();
			}

		}

	}

}

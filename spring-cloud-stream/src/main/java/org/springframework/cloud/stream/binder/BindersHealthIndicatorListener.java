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

package org.springframework.cloud.stream.binder;

import java.util.Map;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.OrderedHealthAggregator;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * {@link BinderFactoryListener} that provides {@link HealthIndicator} support.
 *
 * @author Ilayaperumal Gopinathan
 */
public class BindersHealthIndicatorListener implements BinderFactoryListener {

	private final CompositeHealthIndicator bindersHealthIndicator;

	public BindersHealthIndicatorListener(CompositeHealthIndicator bindersHealthIndicator) {
		this.bindersHealthIndicator = bindersHealthIndicator;
	}

	@Override
	public void apply(String binderConfigurationName, ConfigurableApplicationContext binderProducingContext) {
		if (this.bindersHealthIndicator != null) {
			OrderedHealthAggregator healthAggregator = new OrderedHealthAggregator();
			Map<String, HealthIndicator> indicators = binderProducingContext.getBeansOfType(HealthIndicator.class);
			// if there are no health indicators in the child context, we just mark the binder's health as unknown
			// this can happen due to the fact that configuration is inherited
			HealthIndicator binderHealthIndicator =
					indicators.isEmpty() ? new DefaultHealthIndicator() : new CompositeHealthIndicator(
							healthAggregator, indicators);
			this.bindersHealthIndicator.addHealthIndicator(binderConfigurationName, binderHealthIndicator);
		}
	}

	private static class DefaultHealthIndicator extends AbstractHealthIndicator {

		@Override
		protected void doHealthCheck(Health.Builder builder) throws Exception {
			builder.unknown();
		}
	}
}


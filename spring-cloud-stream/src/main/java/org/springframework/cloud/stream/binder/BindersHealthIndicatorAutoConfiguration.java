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

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.autoconfigure.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.OrderedHealthAggregator;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Ilayaperumal Gopinathan
 */
@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
@ConditionalOnEnabledHealthIndicator("binders")
@AutoConfigureBefore(EndpointAutoConfiguration.class)
@Configuration
public class BindersHealthIndicatorAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean(name = "bindersHealthIndicator")
	public CompositeHealthIndicator bindersHealthIndicator() {
		return new CompositeHealthIndicator(new OrderedHealthAggregator());
	}

	@Bean
	public BinderFactoryListener bindersHealthIndicatorListener(@Qualifier("bindersHealthIndicator") CompositeHealthIndicator compositeHealthIndicator) {
		return new BindersHealthIndicatorListener(compositeHealthIndicator);
	}
}

/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.stub1;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.PingHealthIndicator;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
@Configuration
@EnableConfigurationProperties
public class StubBinder1Configuration {

	@Bean
	@ConfigurationProperties("binder1")
	public Binder<?, ?, ?> binder(BeanFactory beanFactory) {
		StubBinder1 stubBinder1 = new StubBinder1();
		ConfigurableApplicationContext outerContext = null;
		try {
			outerContext = (ConfigurableApplicationContext) beanFactory
					.getBean("outerContext");
		}
		catch (BeansException be) {
			// Pass through
		}
		if (outerContext != null) {
			stubBinder1.setOuterContext(outerContext);
		}
		return stubBinder1;
	}

	@Bean
	public HealthIndicator binderHealthIndicator() {
		return new PingHealthIndicator();
	}

}

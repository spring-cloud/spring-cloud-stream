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

package org.springframework.cloud.stream.config;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.cloud.stream.aggregate.AggregateBuilder;
import org.springframework.cloud.stream.aggregate.AggregateConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Dave Syer
 *
 */
@Configuration
public class AggregateBuilderConfiguration implements CommandLineRunner {

	@Autowired
	private ListableBeanFactory beanFactory;

	@Bean
	public AggregateBuilder aggregateBuilder() {
		return new AggregateBuilder();
	}

	@Override
	public void run(String... args) throws Exception {
		for (AggregateConfigurer configurer : this.beanFactory.getBeansOfType(AggregateConfigurer.class).values()) {
			configurer.configure(aggregateBuilder());
		}
		aggregateBuilder().build();
	}
}

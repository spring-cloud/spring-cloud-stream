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

package demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.aggregate.AggregateBuilder;
import org.springframework.cloud.stream.aggregate.AggregateConfigurer;

import config.SinkModuleDefinition;
import config.SourceModuleDefinition;

@SpringBootApplication
public class DoubleApplication implements AggregateConfigurer {

	@Override
	public void configure(AggregateBuilder builder) {
		builder.from(SourceModuleDefinition.class).as("source")
				.to(SinkModuleDefinition.class).as("sink");
	}

	public static void main(String[] args) {
		SpringApplication.run(DoubleApplication.class, args);
	}

}

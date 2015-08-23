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

package extended;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.aggregate.AggregateBuilder;
import org.springframework.cloud.stream.aggregate.AggregateConfigurer;

import sink.LogSink;
import source.TimeSource;
import transform.LoggingTransformer;

@SpringBootApplication
public class ExtendedApplication implements AggregateConfigurer {

	@Override
	public void configure(AggregateBuilder builder) {
		// @formatter:off
		builder
		.from(TimeSource.class).as("source")
		.via(LoggingTransformer.class)
		.via(LoggingTransformer.class).profiles("other")
		.to(LogSink.class);
		// @formatter:on
	}

	public static void main(String[] args) {
		SpringApplication.run(ExtendedApplication.class, args);
	}

}

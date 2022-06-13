/*
 * Copyright 2020-2022 the original author or authors.
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

package sample.consumer;

import java.util.function.Consumer;

import com.example.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(proxyBeanMethods = false)
public class ConfluentAvroConsumerApplication {

	private final Logger logger = LoggerFactory.getLogger(ConfluentAvroConsumerApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(ConfluentAvroConsumerApplication.class, args);
	}

	@Bean
	Consumer<Sensor> process()  {
		return input -> logger.info("input: {}", input);
	}

}

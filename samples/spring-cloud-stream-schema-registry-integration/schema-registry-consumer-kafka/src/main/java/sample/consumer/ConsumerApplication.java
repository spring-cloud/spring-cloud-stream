/*
 * Copyright 2022 the original author or authors.
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.schema.registry.client.EnableSchemaRegistryClient;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(proxyBeanMethods = false)
@EnableSchemaRegistryClient
public class ConsumerApplication {

	private final Log logger = LogFactory.getLog(getClass());

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

	@Bean
	public Consumer<Sensor> process()  {
		return input -> logger.info("[INPUT-RECEIVED]: " + input);
	}

}

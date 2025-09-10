/*
 * Copyright 2024-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Oleg Zhurakousky
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
	"spring.cloud.function.definition=barConsumer;fooConsumer",
	"spring.kafka.listener.immediate-stop=true",
	"spring.cloud.stream.bindings.fooConsumer-in-0.destination=foo"
})
@EmbeddedKafka
@DirtiesContext
public class KafkaConfigurationTests {


	@Autowired
	private BindingService bindingService;

	@Test
	void testKafkaContainerConfigurationPropagation() throws Exception {
		Binding<?> fooDestination = this.bindingService.getConsumerBindings("fooConsumer-in-0").iterator().next();
		Map<String, Object> fooAdditionalConfigurationProperties = fooDestination.getAdditionalConfigurationProperties();
		assertThat(((Map) fooAdditionalConfigurationProperties.get("containerProperties")).get("stopImmediate")).isEqualTo(true);

		Binding<?> barDestination = this.bindingService.getConsumerBindings("barConsumer-in-0").iterator().next();
		Map<String, Object> barAdditionalConfigurationProperties = barDestination.getAdditionalConfigurationProperties();
		assertThat(((Map) barAdditionalConfigurationProperties.get("containerProperties")).get("stopImmediate")).isEqualTo(true);
	}

	@EnableAutoConfiguration
	@Configuration
	public static class Config {

		@Bean
		Consumer<String> barConsumer() {
			return message -> {
			};
		}
		@Bean
		Consumer<String> fooConsumer() {
			return message -> {
			};
		}
	}
}

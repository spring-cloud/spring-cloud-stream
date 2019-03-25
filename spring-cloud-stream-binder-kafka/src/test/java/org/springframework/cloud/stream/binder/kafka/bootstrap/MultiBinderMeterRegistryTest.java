/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.bootstrap;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
public class MultiBinderMeterRegistryTest {

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 10);

	@Test
	public void testMetricsWorkWithMultiBinders() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				SimpleApplication.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.input.destination=foo",
						"--spring.cloud.stream.bindings.input.binder=inbound",
						"--spring.cloud.stream.bindings.input.group=testGroupabc",
						"--spring.cloud.stream.binders.inbound.type=kafka",
						"--spring.cloud.stream.binders.inbound.environment"
								+ ".spring.cloud.stream.kafka.binder.brokers" + "="
								+ embeddedKafka.getBrokersAsString());

		final MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry.class);

		assertThat(meterRegistry).isNotNull();

		assertThat(meterRegistry.get("spring.cloud.stream.binder.kafka.offset")
				.tag("group", "testGroupabc")
				.tag("topic", "foo").gauge().value()).isNotNull();

		applicationContext.close();
	}

	@SpringBootApplication
	@EnableBinding(Sink.class)
	static class SimpleApplication {

	}

}

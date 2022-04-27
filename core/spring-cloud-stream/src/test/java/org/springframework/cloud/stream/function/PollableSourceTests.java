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

package org.springframework.cloud.stream.function;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.source.MutuallyExclusiveConfigurationPropertiesException;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 *
 * @author Oleg Zhurakousky
 * @author Artem Bilan
 *
 */
public class PollableSourceTests {

	@Test
	void testPollableSource() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PollableAppSampleConfiguration.class))
			.web(WebApplicationType.NONE).run(
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.pollable-source=blah",
				"--spring.integration.poller.cron=*/2 * * * * *",
				"--spring.cloud.stream.poller.max-messages-per-poll=4")) {


			DefaultPollableMessageSource pollableSource = (DefaultPollableMessageSource) context.getBean(PollableMessageSource.class);
			pollableSource.poll(message -> assertThat(message.getPayload()).isNotNull());

			PollerMetadata pollerMetadata = context.getBean(PollerMetadata.class);
			assertThat(pollerMetadata.getTrigger()).isInstanceOf(CronTrigger.class);
			assertThat(TestUtils.getPropertyValue(pollerMetadata.getTrigger(), "expression.expression"))
				.isEqualTo("*/2 * * * * *");
			assertThat(pollerMetadata.getMaxMessagesPerPoll()).isEqualTo(4);
		}
	}

	@Test
	void testPollerDefaultFixedDelay() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PollableAppSampleConfiguration.class))
			.web(WebApplicationType.NONE).run()) {

			PollerMetadata pollerMetadata = context.getBean(PollerMetadata.class);
			assertThat(pollerMetadata.getTrigger()).isInstanceOf(PeriodicTrigger.class);
			assertThat(TestUtils.getPropertyValue(pollerMetadata.getTrigger(), "fixedRate")).isEqualTo(false);
			assertThat(TestUtils.getPropertyValue(pollerMetadata.getTrigger(), "period")).isEqualTo(1000L);
			assertThat(pollerMetadata.getMaxMessagesPerPoll()).isEqualTo(1);
		}
	}

	@Test
	void testPollerProvidedFixedDelayAndMaxMessagesPerPoll() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PollableAppSampleConfiguration.class))
			.web(WebApplicationType.NONE).run(
				"--spring.integration.poller.fixed-delay=7s",
				"--spring.integration.poller.max-messages-per-poll=13")) {

			PollerMetadata pollerMetadata = context.getBean(PollerMetadata.class);
			assertThat(pollerMetadata.getTrigger()).isInstanceOf(PeriodicTrigger.class);
			assertThat(TestUtils.getPropertyValue(pollerMetadata.getTrigger(), "fixedRate")).isEqualTo(false);
			assertThat(TestUtils.getPropertyValue(pollerMetadata.getTrigger(), "period")).isEqualTo(7000L);
			assertThat(pollerMetadata.getMaxMessagesPerPoll()).isEqualTo(13);
		}
	}

	@Test
	void testNoPollerFixedDelayIfFixedRatePresent() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PollableAppSampleConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.integration.poller.fixed-rate=200")) {

			PollerMetadata pollerMetadata = context.getBean(PollerMetadata.class);
			assertThat(pollerMetadata.getTrigger()).isInstanceOf(PeriodicTrigger.class);
			assertThat(TestUtils.getPropertyValue(pollerMetadata.getTrigger(), "fixedRate")).isEqualTo(true);
			assertThat(TestUtils.getPropertyValue(pollerMetadata.getTrigger(), "period")).isEqualTo(200L);
			assertThat(pollerMetadata.getMaxMessagesPerPoll()).isEqualTo(1);
		}
	}

	@Test
	void testPollerMutualProperties() {
		assertThatExceptionOfType(BeanCreationException.class)
			.isThrownBy(() -> new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(PollableAppSampleConfiguration.class))
				.web(WebApplicationType.NONE).run(
					"--spring.integration.poller.fixed-rate=200",
					"--spring.cloud.stream.poller.fixed-delay=300"))
			.withRootCauseExactlyInstanceOf(MutuallyExclusiveConfigurationPropertiesException.class)
			.withMessageContaining("are mutually exclusive");
	}

	@EnableAutoConfiguration
	@Configuration
	public static class PollableAppSampleConfiguration {
	}

}

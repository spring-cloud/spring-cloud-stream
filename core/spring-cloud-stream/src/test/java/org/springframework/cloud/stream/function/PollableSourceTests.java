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

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.scheduling.support.CronTrigger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 * @author Artem Bilan
 *
 */
public class PollableSourceTests {

	@Test
	void test() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(PollableAppSampleConfiguration.class))
						.web(WebApplicationType.NONE).run(
								"--spring.jmx.enabled=false",
								"--spring.cloud.stream.pollable-source=blah",
								"--spring.integration.poller.cron=*/2 * * * * *")) {


			DefaultPollableMessageSource pollableSource = (DefaultPollableMessageSource) context.getBean(PollableMessageSource.class);
			pollableSource.poll(message -> assertThat(message.getPayload()).isNotNull());

			PollerMetadata pollerMetadata = context.getBean(PollerMetadata.class);
			assertThat(pollerMetadata.getTrigger()).isInstanceOf(CronTrigger.class);
			assertThat(TestUtils.getPropertyValue(pollerMetadata.getTrigger(), "expression.expression"))
				.isEqualTo("*/2 * * * * *");
		}
	}

	@EnableAutoConfiguration
	@Configuration
	public static class PollableAppSampleConfiguration {
	}

}

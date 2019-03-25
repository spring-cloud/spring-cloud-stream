/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.config.aggregate;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.config.aggregate.processor.TestProcessor;
import org.springframework.cloud.stream.config.aggregate.source.TestSource;
import org.springframework.cloud.stream.test.binder.TestSupportBinder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
public class AggregateApplicationTests {

	@Test
	@SuppressWarnings("unchecked")
	public void testAggregateApplication() throws Exception {
		ConfigurableApplicationContext context = new AggregateApplicationBuilder(
				AggregateApplicationTestConfig.class).web(false).from(TestSource.class)
						.to(TestProcessor.class).run();
		TestSupportBinder testSupportBinder = (TestSupportBinder) context
				.getBean(BinderFactory.class).getBinder(null, MessageChannel.class);
		MessageChannel processorOutput = testSupportBinder.getChannelForName("output");
		Message<String> received = (Message<String>) (testSupportBinder.messageCollector()
				.forChannel(processorOutput).poll(5, TimeUnit.SECONDS));
		assertThat(received).isNotNull();
		assertThat(received.getPayload().endsWith("processed")).isTrue();

		context.close();
	}

	@Configuration
	@EnableAutoConfiguration
	static class AggregateApplicationTestConfig {

	}

}

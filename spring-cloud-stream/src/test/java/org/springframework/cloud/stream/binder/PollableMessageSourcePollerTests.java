/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.binder.test.TestChannelBinder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.support.PeriodicTrigger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author David Turanski
 **/
public class PollableMessageSourcePollerTests {

	@Test
	public void unconditionalPoller() throws InterruptedException {
		ApplicationContext context =
			new SpringApplicationBuilder(TestChannelBinderConfiguration.getCompleteConfiguration(TestApplication.class))
				.web(WebApplicationType.NONE)
				.run();
		CountDownLatch countDownLatch = context.getBean(CountDownLatch.class);
		countDownLatch.await(1, TimeUnit.SECONDS);
		assertThat(countDownLatch.getCount()).isZero();

	}

	@Test
	public void pollerWithReceiveCondition() throws InterruptedException {
		ApplicationContext context =
			new SpringApplicationBuilder(TestChannelBinderConfiguration.getCompleteConfiguration(TestApplication.class))
				.web(WebApplicationType.NONE)
				.profiles("conditional")
				.run();
		CountDownLatch countDownLatch = context.getBean(CountDownLatch.class);
		countDownLatch.await(1, TimeUnit.SECONDS);
		assertThat(countDownLatch.getCount()).isZero();

	}

	@EnableBinding(PolledSource.class)
	static class TestApplication {

		@Bean
		PollerMetadata pollerMetadata() {
			PollerMetadata pollerMetadata = new PollerMetadata();
			pollerMetadata.setTrigger(new PeriodicTrigger(180, TimeUnit.MILLISECONDS));
			pollerMetadata.setMaxMessagesPerPoll(1);
			return pollerMetadata;
		}

		@Bean
		public CountDownLatch countDownLatch() {
			return new CountDownLatch(5);
		}

		@Bean
		public BeanPostProcessor messageSourceDelegatePostProcessor() {
			return new BeanPostProcessor() {
				final AtomicInteger count = new AtomicInteger();
				@Override
				public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
					if (bean instanceof TestChannelBinder) {
						((TestChannelBinder) bean).setMessageSourceDelegate(() ->
							new GenericMessage<>(("polled data-" + count.getAndIncrement()).getBytes(),
								Collections.singletonMap("contentType", "text/plain"))
						);
					}
					return bean;
				}
			};
		}

		@Bean
		@Profile("!conditional")
		MessageHandler messageHandler(CountDownLatch countDownLatch) {
			return m -> {
				assertThat((String) m.getPayload()).startsWith("polled data");
				countDownLatch.countDown();
			};
		}

		@Profile("conditional")
		@Configuration
		static class ConditionalConsumerConfig {

			@Bean
			BooleanSupplier conditionalConsumer(CountDownLatch countDownLatch) {
				return () -> {
					boolean ready = countDownLatch.getCount() % 2 == 1;
					countDownLatch.countDown();
					return ready;
				};
			}

			@Bean
			MessageHandler messageHandler() {
				return m -> {
					System.out.println("! received message " + m);
					assertThat((String) m.getPayload()).matches("polled data-[0,1]");
				};
			}
		}

		@Bean
		public PollableMessageSourcePoller pollingEndpoint(PollableMessageSource source,
			PollerMetadata pollerMetadata, MessageHandler messageHandler, @Nullable BooleanSupplier receiveCondition) {
			PollableMessageSourcePoller.Builder builder = PollableMessageSourcePoller.builder()
				.messageSource(source)
				.messageHandler(messageHandler)
				.pollerMetadata(pollerMetadata)
				.parameterizedTypeReference(new ParameterizedTypeReference<String>() {
				});

			if (receiveCondition != null) {
				builder.receiveCondition(receiveCondition);

			}

			return builder.build();
		}

	}

	public interface PolledSource {

		@Input
		PollableMessageSource source();

	}

}

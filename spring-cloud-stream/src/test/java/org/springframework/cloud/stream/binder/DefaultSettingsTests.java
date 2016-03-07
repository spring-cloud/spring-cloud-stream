/*
 * Copyright 2016 the original author or authors.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;

/**
 * @author Marius Bogoevici
 */
public class DefaultSettingsTests {

	@SuppressWarnings("unchecked")
	@Test
	public void testDefaultSettings() {
		ConfigurableApplicationContext applicationContext = createBuilder().run(
				"--spring.cloud.stream.bindings.foo.destination=fooDest",
				"--spring.cloud.stream.bindings.bar.destination=barDest",
				"--spring.cloud.stream.bindings.baz.destination=bazDest",
				"--spring.cloud.stream.bindings.qux.destination=quxDest",
				"--spring.cloud.stream.bindings.foo.group=fooBarGroup",
				"--spring.cloud.stream.bindings.bar.group=fooBarGroup",
				"--spring.cloud.stream.consumerDefaults.concurrency=2",
				"--spring.cloud.stream.producerDefaults.requiredGroups=quxbazReq1,quxbazReq2");

		Binder binder = applicationContext.getBean(Binder.class);
		FooChannels fooChannels = applicationContext.getBean(FooChannels.class);

		ArgumentCaptor<ConsumerProperties> fooConsumerProperties = ArgumentCaptor.forClass(ConsumerProperties.class);
		verify(binder).bindConsumer(eq("fooDest"), eq("fooBarGroup"), same(fooChannels.foo()),
				fooConsumerProperties.capture());
		assertThat(fooConsumerProperties.getValue().getConcurrency(), CoreMatchers.equalTo(2));
		ArgumentCaptor<ConsumerProperties> barConsumerProperties = ArgumentCaptor.forClass(ConsumerProperties.class);
		verify(binder).bindConsumer(eq("barDest"), eq("fooBarGroup"), same(fooChannels.bar()),
				barConsumerProperties.capture());
		assertThat(barConsumerProperties.getValue().getConcurrency(), CoreMatchers.equalTo(2));

		ArgumentCaptor<ProducerProperties> bazProducerProperties = ArgumentCaptor.forClass(ProducerProperties.class);
		verify(binder).bindProducer(eq("bazDest"), same(fooChannels.baz()), bazProducerProperties.capture());
		assertThat(bazProducerProperties.getValue().getRequiredGroups(), arrayContaining("quxbazReq1", "quxbazReq2"));

		ArgumentCaptor<ProducerProperties> quxProducerProperties = ArgumentCaptor.forClass(ProducerProperties.class);
		verify(binder).bindProducer(eq("quxDest"), same(fooChannels.qux()), quxProducerProperties.capture());
		assertThat(quxProducerProperties.getValue().getRequiredGroups(), arrayContaining("quxbazReq1", "quxbazReq2"));

		verifyNoMoreInteractions(binder);
		applicationContext.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDefaultSettingsOverridden() {
		ConfigurableApplicationContext applicationContext = createBuilder().run(
				"--spring.cloud.stream.bindings.foo.destination=fooDest",
				"--spring.cloud.stream.bindings.bar.destination=barDest",
				"--spring.cloud.stream.bindings.baz.destination=bazDest",
				"--spring.cloud.stream.bindings.qux.destination=quxDest",
				"--spring.cloud.stream.bindings.foo.group=fooBarGroup",
				"--spring.cloud.stream.bindings.bar.group=fooBarGroup",
				"--spring.cloud.stream.consumerDefaults.concurrency=2",
				"--spring.cloud.stream.bindings.bar.concurrency=4",
				"--spring.cloud.stream.producerDefaults.requiredGroups=quxbazReq1,quxbazReq2",
				"--spring.cloud.stream.bindings.qux.requiredGroups=quxbazReq3,quxbazReq4");

		Binder binder = applicationContext.getBean(Binder.class);
		FooChannels fooChannels = applicationContext.getBean(FooChannels.class);

		ArgumentCaptor<ConsumerProperties> fooConsumerProperties = ArgumentCaptor.forClass(ConsumerProperties.class);
		verify(binder).bindConsumer(eq("fooDest"), eq("fooBarGroup"), same(fooChannels.foo()),
				fooConsumerProperties.capture());
		assertThat(fooConsumerProperties.getValue().getConcurrency(), CoreMatchers.equalTo(2));
		ArgumentCaptor<ConsumerProperties> barConsumerProperties = ArgumentCaptor.forClass(ConsumerProperties.class);
		verify(binder).bindConsumer(eq("barDest"), eq("fooBarGroup"), same(fooChannels.bar()),
				barConsumerProperties.capture());
		assertThat(barConsumerProperties.getValue().getConcurrency(), CoreMatchers.equalTo(4));

		ArgumentCaptor<ProducerProperties> bazProducerProperties = ArgumentCaptor.forClass(ProducerProperties.class);
		verify(binder).bindProducer(eq("bazDest"), same(fooChannels.baz()), bazProducerProperties.capture());
		assertThat(bazProducerProperties.getValue().getRequiredGroups(), arrayContaining("quxbazReq1", "quxbazReq2"));

		ArgumentCaptor<ProducerProperties> quxProducerProperties = ArgumentCaptor.forClass(ProducerProperties.class);
		verify(binder).bindProducer(eq("quxDest"), same(fooChannels.qux()), quxProducerProperties.capture());
		assertThat(quxProducerProperties.getValue().getRequiredGroups(), arrayContaining("quxbazReq3", "quxbazReq4"));

		verifyNoMoreInteractions(binder);
		applicationContext.close();
	}

	private SpringApplicationBuilder createBuilder() {
		return new SpringApplicationBuilder(TestFooChannels.class)
				.web(false);
	}


	@EnableBinding(FooChannels.class)
	@EnableAutoConfiguration
	@Import(MockBinderRegistryConfiguration.class)
	public static class TestFooChannels {

	}
}

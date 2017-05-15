/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.partitioning;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.config.BinderFactoryConfiguration;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Janne Valkealahti
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PartitionedConsumerTest.TestSink.class)
public class PartitionedConsumerTest {

	@Autowired
	private BinderFactory binderFactory;

	@Autowired
	@Bindings(TestSink.class)
	private Sink testSink;

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testBindingPartitionedConsumer() {
		Binder binder = this.binderFactory.getBinder(null, MessageChannel.class);
		ArgumentCaptor<ConsumerProperties> argumentCaptor = ArgumentCaptor.forClass(ConsumerProperties.class);
		verify(binder).bindConsumer(eq("partIn"), isNull(), eq(this.testSink.input()),
				argumentCaptor.capture());
		Assert.assertThat(argumentCaptor.getValue().getInstanceIndex(), equalTo(0));
		Assert.assertThat(argumentCaptor.getValue().getInstanceCount(), equalTo(2));
		verifyNoMoreInteractions(binder);
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	@Import({MockBinderRegistryConfiguration.class, BinderFactoryConfiguration.class})
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/partitioned-consumer-test.properties")
	public static class TestSink {

	}

	class PropertiesArgumentMatcher implements ArgumentMatcher<ConsumerProperties> {

		@Override
		public boolean matches(ConsumerProperties argument) {
			return argument instanceof ConsumerProperties;
		}
	}

}

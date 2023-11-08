/*
 * Copyright 2015-2023 the original author or authors.
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

package org.springframework.cloud.stream.partitioning;

import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Janne Valkealahti
 * @author Soby Chacko
 */
@SpringBootTest(classes = PartitionedConsumerTest.TestSink.class, properties = "spring.cloud.stream.default-binder=mock")
class PartitionedConsumerTest {

	@Autowired
	private BinderFactory binderFactory;

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	void bindingPartitionedConsumer() {
		Binder binder = this.binderFactory.getBinder(null, MessageChannel.class);
		ArgumentCaptor<ConsumerProperties> argumentCaptor = ArgumentCaptor
				.forClass(ConsumerProperties.class);
		ArgumentCaptor<DirectWithAttributesChannel> directWithAttributesChannelArgumentCaptor =
			ArgumentCaptor.forClass(DirectWithAttributesChannel.class);
		verify(binder).bindConsumer(eq("partIn"), isNull(), directWithAttributesChannelArgumentCaptor.capture(),
				argumentCaptor.capture());
		assertThat(directWithAttributesChannelArgumentCaptor.getValue().getBeanName()).isEqualTo("sink-in-0");
		assertThat(argumentCaptor.getValue().getInstanceIndex()).isEqualTo(0);
		assertThat(argumentCaptor.getValue().getInstanceCount()).isEqualTo(2);
		verifyNoMoreInteractions(binder);
	}

	@EnableAutoConfiguration
	@Import({ BinderFactoryAutoConfiguration.class })
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/partitioned-consumer-test.properties")
	public static class TestSink {

		@Bean
		public Consumer<String> sink() {
			return System.out::println;
		}
	}
}

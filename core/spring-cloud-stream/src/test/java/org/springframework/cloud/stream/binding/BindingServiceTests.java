/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

class BindingServiceTests {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void unbindsConsumerBindingsIfLaterConsumerBindingFails() {
		BindingServiceProperties properties = new BindingServiceProperties();
		properties.setBindingRetryInterval(0);
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo,bar");
		String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		properties.setBindings(bindingProperties);

		Binder<MessageChannel, ConsumerProperties, ?> binder = Mockito.mock(Binder.class);
		BinderFactory binderFactory = Mockito.mock(BinderFactory.class);
		MessageChannel inputChannel = new DirectChannel();
		doReturn(binder).when(binderFactory).getBinder(isNull(), eq(DirectChannel.class));
		BindingService service = new BindingService(properties, binderFactory, new ObjectMapper());
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);

		when(binder.bindConsumer(eq("foo"), isNull(), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding);
		when(binder.bindConsumer(eq("bar"), isNull(), same(inputChannel),
				any(ConsumerProperties.class)))
			.thenThrow(new RuntimeException("fail"));

		assertThatThrownBy(() -> service.bindConsumer(inputChannel, inputChannelName))
			.isInstanceOf(RuntimeException.class)
			.hasMessage("fail");

		InOrder inOrder = Mockito.inOrder(mockBinding);
		inOrder.verify(mockBinding).stop();
		inOrder.verify(mockBinding).unbind();
		assertThat(service.getConsumerBindings(inputChannelName)).isEmpty();
	}

}

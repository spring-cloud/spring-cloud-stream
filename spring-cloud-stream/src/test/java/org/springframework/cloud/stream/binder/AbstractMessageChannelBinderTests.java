/*
 * Copyright 2017 the original author or authors.
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

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

/**
 * @author Marius Bogoevici
 * @since 1.2.2
 */
public class AbstractMessageChannelBinderTests {

	@Test
	public void testEndpointLifecycle() throws Exception {
		StubMessageChannelBinder binder = new StubMessageChannelBinder();
		binder.setApplicationContext(new GenericApplicationContext());

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo", "fooGroup", new DirectChannel(),
				new ConsumerProperties());
		DirectFieldAccessor consumerBindingAccessor = new DirectFieldAccessor(consumerBinding);
		Object messageProducer = consumerBindingAccessor.getPropertyValue("lifecycle");
		Mockito.verify((Lifecycle) messageProducer).start();
		Mockito.verify((InitializingBean) messageProducer).afterPropertiesSet();
		Mockito.verify((MessageProducer) messageProducer).setOutputChannel(Mockito.any(MessageChannel.class));
		Mockito.verifyNoMoreInteractions(messageProducer);
		consumerBinding.unbind();
		Mockito.verify((Lifecycle) messageProducer).stop();
		Mockito.verify((DisposableBean) messageProducer).destroy();
		Mockito.verifyNoMoreInteractions(messageProducer);

		Binding<MessageChannel> producerBinding = binder.bindProducer("bar", new DirectChannel(),
				new ProducerProperties());
		DirectFieldAccessor producerBindingAccessor = new DirectFieldAccessor(producerBinding);
		Object messageHandler = producerBindingAccessor.getPropertyValue("lifecycle");
		Mockito.verify((Lifecycle) messageHandler).start();
		Mockito.verify((InitializingBean) messageHandler).afterPropertiesSet();
		Mockito.verifyNoMoreInteractions(messageHandler);
		producerBinding.unbind();
		Mockito.verify((Lifecycle) messageHandler).stop();
		Mockito.verify((DisposableBean) messageHandler).destroy();
		Mockito.verifyNoMoreInteractions(messageHandler);
	}

	private static class StubMessageChannelBinder extends
			AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties, ProvisioningProvider<ConsumerProperties, ProducerProperties>> {

		@SuppressWarnings("unchecked")
		private StubMessageChannelBinder() {
			super(true, null, Mockito.mock(ProvisioningProvider.class));
		}

		@Override
		protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
				ProducerProperties producerProperties) throws Exception {
			MessageHandler mock = Mockito.mock(MessageHandler.class, Mockito.withSettings()
					.extraInterfaces(Lifecycle.class, InitializingBean.class, DisposableBean.class));
			return mock;
		}

		@Override
		protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
				ConsumerProperties properties) throws Exception {
			return Mockito.mock(MessageProducer.class,
					Mockito.withSettings().extraInterfaces(Lifecycle.class, InitializingBean.class,
							DisposableBean.class));
		}
	}

}

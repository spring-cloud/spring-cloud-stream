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

package org.springframework.cloud.stream.binder.integration;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.integration.SpringIntegrationProvisioner.SpringIntegrationConsumerDestination;
import org.springframework.cloud.stream.binder.integration.SpringIntegrationProvisioner.SpringIntegrationProducerDestination;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;

/**
 * Implementation of {@link Binder} backed by Spring Integration framework.
 * It is useful for localized demos and testing.
 * <p>
 * This binder extends from the same base class ({@link AbstractMessageChannelBinder}) as
 * other binders (i.e., Rabbit, Kafka etc). Interaction with this binder is done via a pair of named {@link MessageChannel}s.
 * <br>
 * The names of the channels are:
 * <ul>
 * <li>{@link SpringIntegrationBinderConfiguration#BINDER_INPUT}</li>
 * <li>{@link SpringIntegrationBinderConfiguration#BINDER_OUTPUT}</li>
 * </ul>
 * Simply autowire them in your your application and send/receive messages.
 * </p>
 * You must also add {@link SpringIntegrationBinderConfiguration} to your configuration.
 * Below is the example using Spring Boot test.
 * <pre class="code">
 *
 * &#064;RunWith(SpringJUnit4ClassRunner.class)
 * &#064;SpringBootTest(classes = {SpringIntegrationBinderConfiguration.class, TestWithSIBinder.MyProcessor.class})
 * public class TestWithSIBinder {
 *     &#064;Autowired
 *     private MessageChannel BINDER_INPUT;
 *
 *     &#064;Autowired
 *     private QueueChannel BINDER_OUTPUT;
 *
 *     &#064;Test
 *     public void testWiring() {
 *         BINDER_INPUT.send(new GenericMessage<String>("Hello"));
 *         assertEquals("Hello world", new String((byte[])BINDER_OUTPUT.receive().getPayload(), StandardCharsets.UTF_8));
 *     }
 *
 *     &#064;SpringBootApplication
 *     &#064;EnableBinding(Processor.class)
 *     public static class MyProcessor {
 *         &#064;Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
 *         public String transform(String in) {
 *             return in + " world";
 *         }
 *     }
 * }
 * </pre>
 *
 * @author Oleg Zhurakousky
 */
class SpringIntegrationChannelBinder extends AbstractMessageChannelBinder<ConsumerProperties,
	ProducerProperties, SpringIntegrationProvisioner> {

	@Autowired
	private BeanFactory beanFactory;

	SpringIntegrationChannelBinder(SpringIntegrationProvisioner provisioningProvider) {
		super(provisioningProvider);
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ProducerProperties producerProperties, MessageChannel errorChannel) throws Exception {
		BridgeHandler handler = new BridgeHandler();
		handler.setBeanFactory(this.beanFactory);
		handler.setOutputChannel(((SpringIntegrationProducerDestination)destination).getChannel());
		return handler;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group, ConsumerProperties properties)
			throws Exception {
		SubscribableChannel siBinderInputChannel = ((SpringIntegrationConsumerDestination)destination).getChannel();
		BridgeHandler handler = new BridgeHandler();
		handler.setBeanFactory(this.beanFactory);
		siBinderInputChannel.subscribe(handler);
		return handler;
	}
}

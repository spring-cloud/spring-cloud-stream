/*
 * Copyright 2022-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar;

import java.util.Optional;
import java.util.Set;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarConsumerProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarExtendedBindingProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarProducerProperties;
import org.springframework.cloud.stream.binder.pulsar.provisioning.PulsarTopicProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.AbstractMessageProducingHandler;
import org.springframework.integration.support.management.ManageableLifecycle;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TypedMessageBuilderCustomizer;
import org.springframework.pulsar.listener.AbstractPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.listener.PulsarRecordMessageListener;
import org.springframework.pulsar.support.header.PulsarHeaderMapper;

/**
 * {@link Binder} implementation for Apache Pulsar.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<PulsarConsumerProperties>, ExtendedProducerProperties<PulsarProducerProperties>, PulsarTopicProvisioner>
		implements ExtendedPropertiesBinder<MessageChannel, PulsarConsumerProperties, PulsarProducerProperties> {

	private final PulsarTemplate<Object> pulsarTemplate;

	private final PulsarConsumerFactory<?> pulsarConsumerFactory;

	private final PulsarBinderConfigurationProperties binderConfigProps;

	private final SchemaResolver schemaResolver;

	private final PulsarHeaderMapper headerMapper;

	private PulsarExtendedBindingProperties extendedBindingProperties = new PulsarExtendedBindingProperties();

	public PulsarMessageChannelBinder(PulsarTopicProvisioner provisioningProvider,
			PulsarTemplate<Object> pulsarTemplate, PulsarConsumerFactory<?> pulsarConsumerFactory,
			PulsarBinderConfigurationProperties binderConfigProps, SchemaResolver schemaResolver,
			PulsarHeaderMapper headerMapper) {
		super(null, provisioningProvider);
		this.pulsarTemplate = pulsarTemplate;
		this.pulsarConsumerFactory = pulsarConsumerFactory;
		this.binderConfigProps = binderConfigProps;
		this.schemaResolver = schemaResolver;
		this.headerMapper = headerMapper;
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ExtendedProducerProperties<PulsarProducerProperties> producerProperties, MessageChannel errorChannel) {
		final Schema<Object> schema;
		if (producerProperties.isUseNativeEncoding()) {
			var schemaType = Optional.ofNullable(producerProperties.getExtension().getSchemaType())
					.orElse(SchemaType.NONE);
			schema = this.schemaResolver
					.resolveSchema(schemaType, producerProperties.getExtension().getMessageType(),
							producerProperties.getExtension().getMessageKeyType())
					.orElseThrow(() -> "Could not determine producer schema for " + destination.getName());
		}
		else {
			schema = null;
		}

		var layeredProducerProps = PulsarBinderUtils.mergeModifiedProducerProperties(
				this.binderConfigProps.getProducer(), producerProperties.getExtension());
		var handler = new PulsarProducerConfigurationMessageHandler(this.pulsarTemplate, schema, destination.getName(),
				(builder) -> PulsarBinderUtils.loadConf(builder, layeredProducerProps),
				determineOutboundHeaderMapper(producerProperties));
		handler.setApplicationContext(getApplicationContext());
		handler.setBeanFactory(getBeanFactory());

		return handler;
	}

	@Nullable
	private PulsarBinderHeaderMapper determineOutboundHeaderMapper(
			ExtendedProducerProperties<PulsarProducerProperties> extProducerProps) {
		if (HeaderMode.none.equals(extProducerProps.getHeaderMode())) {
			return null;
		}
		return new PulsarBinderHeaderMapper(this.headerMapper);
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
			ExtendedConsumerProperties<PulsarConsumerProperties> properties) {
		var containerProperties = new PulsarContainerProperties();
		containerProperties.setTopics(Set.of(destination.getName()));

		var inboundHeaderMapper = determineInboundHeaderMapper(properties);

		var messageDrivenChannelAdapter = new PulsarMessageDrivenChannelAdapter();
		containerProperties.setMessageListener((PulsarRecordMessageListener<?>) (consumer, pulsarMsg) -> {
			var springMessage = (inboundHeaderMapper != null)
					? MessageBuilder.createMessage(pulsarMsg.getValue(), inboundHeaderMapper.toSpringHeaders(pulsarMsg))
					: MessageBuilder.withPayload(pulsarMsg.getValue()).build();
			messageDrivenChannelAdapter.send(springMessage);
		});

		if (properties.isUseNativeDecoding()) {
			var schemaType = Optional.ofNullable(properties.getExtension().getSchemaType()).orElse(SchemaType.NONE);
			var schema = this.schemaResolver
					.resolveSchema(schemaType, properties.getExtension().getMessageType(),
							properties.getExtension().getMessageKeyType())
					.orElseThrow(() -> "Could not determine consumer schema for " + destination.getName());
			containerProperties.setSchema(schema);
		}
		else {
			containerProperties.setSchema(Schema.BYTES);
		}
		var subscriptionName = PulsarBinderUtils.subscriptionName(properties.getExtension(), destination);
		containerProperties.setSubscriptionName(subscriptionName);

		var layeredConsumerProps = PulsarBinderUtils
				.mergeModifiedConsumerProperties(this.binderConfigProps.getConsumer(), properties.getExtension());
		containerProperties.getPulsarConsumerProperties().putAll(layeredConsumerProps);
		containerProperties.updateContainerProperties();

		var container = new DefaultPulsarMessageListenerContainer<>(this.pulsarConsumerFactory, containerProperties);
		messageDrivenChannelAdapter.setMessageListenerContainer(container);
		messageDrivenChannelAdapter.setApplicationContext(getApplicationContext());
		messageDrivenChannelAdapter.setBeanFactory(getApplicationContext().getBeanFactory());
		return messageDrivenChannelAdapter;
	}

	@Nullable
	private PulsarBinderHeaderMapper determineInboundHeaderMapper(
			ExtendedConsumerProperties<PulsarConsumerProperties> extConsumerProps) {
		if (HeaderMode.none.equals(extConsumerProps.getHeaderMode())) {
			return null;
		}
		return new PulsarBinderHeaderMapper(this.headerMapper);
	}

	@Override
	public PulsarConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public PulsarProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	public String getDefaultsPrefix() {
		return null;
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return null;
	}

	public PulsarExtendedBindingProperties getExtendedBindingProperties() {
		return this.extendedBindingProperties;
	}

	public void setExtendedBindingProperties(PulsarExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	static class PulsarMessageDrivenChannelAdapter extends MessageProducerSupport {

		AbstractPulsarMessageListenerContainer<?> messageListenerContainer;

		public void send(Message<?> message) {
			sendMessage(message);
		}

		@Override
		protected void doStart() {
			this.messageListenerContainer.start();
		}

		@Override
		protected void doStop() {
			this.messageListenerContainer.stop();
		}

		public void setMessageListenerContainer(AbstractPulsarMessageListenerContainer<?> messageListenerContainer) {
			this.messageListenerContainer = messageListenerContainer;
		}

	}

	static class PulsarProducerConfigurationMessageHandler extends AbstractMessageProducingHandler
			implements ManageableLifecycle {

		private final PulsarTemplate<Object> pulsarTemplate;

		private final Schema<Object> schema;

		private final String destination;

		private final ProducerBuilderCustomizer<Object> layeredProducerPropsCustomizer;

		private final PulsarHeaderMapper headerMapper;

		private boolean running = true;

		PulsarProducerConfigurationMessageHandler(PulsarTemplate<Object> pulsarTemplate, Schema<Object> schema,
				String destination, ProducerBuilderCustomizer<Object> layeredProducerPropsCustomizer,
				PulsarHeaderMapper headerMapper) {
			this.pulsarTemplate = pulsarTemplate;
			this.schema = schema;
			this.destination = destination;
			this.layeredProducerPropsCustomizer = layeredProducerPropsCustomizer;
			this.headerMapper = headerMapper;
		}

		@Override
		public void start() {
			try {
				super.onInit();
			}
			catch (Exception ex) {
				this.logger.error(ex, "Initialization errors: ");
				throw new RuntimeException(ex);
			}
		}

		@Override
		public void stop() {
			// TODO - should we close the underlyiung producer?
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}

		@Override
		protected void handleMessageInternal(Message<?> message) {
			try {
				// @formatter:off
				this.pulsarTemplate.newMessage(message.getPayload())
						.withTopic(this.destination)
						.withSchema(this.schema)
						.withProducerCustomizer(this.layeredProducerPropsCustomizer)
						.withMessageCustomizer(this.applySpringHeadersAsPulsarProperties(message.getHeaders()))
						.sendAsync();
				// @formatter:on
			}
			catch (Exception ex) {
				logger.trace(ex, "Failed to send message to destination: " + this.destination);
			}
		}

		private TypedMessageBuilderCustomizer<Object> applySpringHeadersAsPulsarProperties(MessageHeaders headers) {
			return (mb) -> {
				if (this.headerMapper != null) {
					this.headerMapper.toPulsarHeaders(headers).forEach(mb::property);
				}
			};
		}

	}

}

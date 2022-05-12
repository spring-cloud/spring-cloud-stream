/*
 * Copyright 2021-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.reactorkafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.support.ConsumerConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.support.ProducerConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.utils.BindingUtils;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Gary Russell
 * @since 4.0
 *
 */
public class ReactorKafkaBinder
		extends AbstractMessageChannelBinder<ExtendedConsumerProperties<KafkaConsumerProperties>,
				ExtendedProducerProperties<KafkaProducerProperties>, KafkaTopicProvisioner>
		implements
		ExtendedPropertiesBinder<MessageChannel, KafkaConsumerProperties, KafkaProducerProperties> {

	private static final Log logger = LogFactory.getLog(ReactorKafkaBinder.class);

	private final KafkaBinderConfigurationProperties configurationProperties;

	private KafkaExtendedBindingProperties extendedBindingProperties = new KafkaExtendedBindingProperties();

	private ConsumerConfigCustomizer consumerConfigCustomizer;

	private ProducerConfigCustomizer producerConfigCustomizer;

	public ReactorKafkaBinder(KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioner) {

		super(new String[0], provisioner, null, null);
		this.configurationProperties = configurationProperties;
	}

	public void setConsumerConfigCustomizer(ConsumerConfigCustomizer consumerConfigCustomizer) {
		this.consumerConfigCustomizer = consumerConfigCustomizer;
	}

	public void setProducerConfigCustomizer(ProducerConfigCustomizer producerConfigCustomizer) {
		this.producerConfigCustomizer = producerConfigCustomizer;
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties, MessageChannel errorChannel)
			throws Exception {

		Map<String, Object> configs = BindingUtils.createProducerConfigs(producerProperties,
				this.configurationProperties);
		if (this.producerConfigCustomizer != null) {
			this.producerConfigCustomizer.configure(configs, producerProperties.getBindingName(),
					destination.getName());
		}

		SenderOptions<Object, Object> opts = SenderOptions.create(configs);
		// TODO bean for converter; MCB doesn't use one on the producer side.
		RecordMessageConverter converter = new MessagingMessageConverter();
		return new ReactorMessageHandler(opts, converter, destination.getName());
	}


	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) throws Exception {

		boolean anonymous = !StringUtils.hasText(group);
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString() : group;
		Map<String, Object> configs = BindingUtils.createConsumerConfigs(anonymous, consumerGroup, properties,
				this.configurationProperties);

		if (this.consumerConfigCustomizer != null) {
			this.consumerConfigCustomizer.configure(configs, properties.getBindingName(), destination.getName());
		}

		MessageConverter converter = BindingUtils.getConsumerMessageConverter(getApplicationContext(), properties,
				this.configurationProperties);
		Assert.isInstanceOf(RecordMessageConverter.class, converter);
		ReceiverOptions<Object, Object> opts = ReceiverOptions.create(configs)
			.addAssignListener(parts -> logger.info("Assigned: " + parts))
			.subscription(Collections.singletonList(destination.getName()));

		class ReactorMessageProducer extends MessageProducerSupport {

			private final List<KafkaReceiver<Object, Object>> receivers = new ArrayList<>();

			ReactorMessageProducer() {
				for (int i = 0; i < properties.getConcurrency(); i++) {
					this.receivers.add(KafkaReceiver.create(opts));
				}
			}

			@SuppressWarnings("unchecked")
			@Override
			protected void doStart() {
				List<Flux<Message<Object>>> fluxes = new ArrayList<>();
				int concurrency = properties.getConcurrency();
				for (int i = 0; i < concurrency; i++) {
					fluxes.add(this.receivers.get(i)
							.receive()
							.map(record -> (Message<Object>) ((RecordMessageConverter) converter)
									.toMessage(record, null, null, null)));
				}
				if (concurrency == 1) {
					subscribeToPublisher(fluxes.get(0));
				}
				else {
					subscribeToPublisher(Flux.merge(fluxes));
				}
			}

		}
		return new ReactorMessageProducer();
	}

	@Override
	public KafkaConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	public String getDefaultsPrefix() {
		return this.extendedBindingProperties.getDefaultsPrefix();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return this.extendedBindingProperties.getExtendedPropertiesEntryClass();
	}

	public void setExtendedBindingProperties(
			KafkaExtendedBindingProperties extendedBindingProperties) {

		this.extendedBindingProperties = extendedBindingProperties;
	}

	private static class ReactorMessageHandler extends AbstractMessageHandler implements Lifecycle {

		private final RecordMessageConverter converter;

		private final String topic;

		private final SenderOptions<Object, Object> senderOptions;

		private volatile KafkaSender<Object, Object> sender;

		private volatile boolean running;

		ReactorMessageHandler(SenderOptions<Object, Object> opts, RecordMessageConverter converter,
				String topic) {

			this.senderOptions = opts;
			this.converter = converter;
			this.topic = topic;
		}

		@Override
		protected void handleMessageInternal(Message<?> message) {
			Object sendResultHeader = message.getHeaders().get("sendResult");
			Sinks.One<RecordMetadata> sink = Sinks.one();
			if (sendResultHeader instanceof AtomicReference) {
				@SuppressWarnings("unchecked")
				AtomicReference<Mono<RecordMetadata>> result =
						(AtomicReference<Mono<RecordMetadata>>) sendResultHeader;
				result.set(sink.asMono());
			}
			if (this.sender != null) {
				UUID uuid = UUID.randomUUID();
				@SuppressWarnings("unchecked")
				SenderRecord<Object, Object, UUID> sr = SenderRecord.create(
						(ProducerRecord<Object, Object>) converter.fromMessage(message, topic), uuid);
				Flux<SenderResult<UUID>> result = sender.send(Flux.just(sr));
				result.subscribe(res -> sink.emitValue(res.recordMetadata(), null));
			}
			else {
				sink.emitError(new IllegalStateException("Handler is not running"), null);
			}
		}

		@Override
		public synchronized void start() {
			if (!this.running) {
				this.sender = KafkaSender.create(this.senderOptions);
				this.running = true;
			}
		}

		@Override
		public synchronized void stop() {
			if (this.running) {
				KafkaSender<Object, Object> theSender = this.sender;
				this.sender = null;
				theSender.close();
				this.running = false;
			}
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}

	}

}

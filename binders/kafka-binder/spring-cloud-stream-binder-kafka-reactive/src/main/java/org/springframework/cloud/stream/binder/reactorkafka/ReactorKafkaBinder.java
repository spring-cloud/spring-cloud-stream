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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
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

	private static final Log log = LogFactory.getLog(ReactorKafkaBinder.class);

	private final KafkaBinderConfigurationProperties configurationProperties;

	private KafkaExtendedBindingProperties extendedBindingProperties = new KafkaExtendedBindingProperties();

	public ReactorKafkaBinder(KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioner) {

		super(new String[0], provisioner, null, null);
		this.configurationProperties = configurationProperties;
	}


	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties, MessageChannel errorChannel)
			throws Exception {

		Map<String, Object> configs = createProducerConfigs(producerProperties);
		// TODO: Move config customizers to core
//		if (this.producerConfigCustomizer != null) {
//			this.producerConfigCustomizer.configure(props, bindingNameHolder.get(), destination);
//			bindingNameHolder.remove();
//		}

		SenderOptions<Object, Object> opts = SenderOptions.create(configs);
		KafkaSender<Object, Object> sender = KafkaSender.create(opts);
		// TODO bean?
		RecordMessageConverter converter = new MessagingMessageConverter();
		// TODO Lifecycle etc
		return message -> {
			Object sendResultHeader = message.getHeaders().get("sendResult");
			Sinks.One<RecordMetadata> sink = Sinks.one();
			if (sendResultHeader instanceof AtomicReference) {
				AtomicReference<Mono<RecordMetadata>> result =
						(AtomicReference<Mono<RecordMetadata>>) sendResultHeader;
				result.set(sink.asMono());
			}
			UUID uuid = UUID.randomUUID();
			@SuppressWarnings("unchecked")
			SenderRecord<Object, Object, UUID> sr = SenderRecord.create(
					(ProducerRecord<Object, Object>) converter.fromMessage(message, destination.getName()), uuid);
			Flux<SenderResult<UUID>> result = sender.send(Flux.just(sr));
			result.subscribe(res -> sink.emitValue(res.recordMetadata(), null));
		};
	}


	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) throws Exception {

		boolean anonymous = !StringUtils.hasText(group);
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString() : group;
		Map<String, Object> configs = createConsumerConfigs(anonymous, consumerGroup, properties);

		// TODO: Move config customizers to core
//		if (this.consumerConfigCustomizer != null) {
//			this.consumerConfigCustomizer.configure(configs, bindingNameHolder.get(), destination);
//		}

		ReceiverOptions<Object, Object> opts = ReceiverOptions.create(configs)
			.addAssignListener(parts -> System.out.println("Assigned: " + parts))
			.subscription(Collections.singletonList(destination.getName()));

		Flux<Message<Object>> flux = KafkaReceiver.create(opts)
				.receive()
				// TODO: ReceiverRecord to Message Mapper
				.map(rec -> {
					log.info(rec.value().toString());
					return rec;
				})
				.map(record -> MessageBuilder.withPayload(record.value()).build())
				.map(msg -> {
					log.info(msg.toString());
					return msg;
				});

		return new MessageProducerSupport() {

			@Override
			protected void doStart() {
				subscribeToPublisher(flux);
			}

		};
	}

	/*
	 * TODO: Copied from Kafka binder - refactor to core
	 */
	private Map<String, Object> createConsumerConfigs(boolean anonymous, String consumerGroup,
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties) {

		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				anonymous ? "latest" : "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

		Map<String, Object> mergedConfig = this.configurationProperties
				.mergedConsumerConfiguration();
		if (!ObjectUtils.isEmpty(mergedConfig)) {
			props.putAll(mergedConfig);
		}
		if (ObjectUtils.isEmpty(props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.configurationProperties.getKafkaConnectionString());
		}
		Map<String, String> config = consumerProperties.getExtension().getConfiguration();
		if (!ObjectUtils.isEmpty(config)) {
			Assert.state(!config.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
					ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " cannot be overridden at the binding level; "
							+ "use multiple binders instead");
			props.putAll(config);
		}
		if (!ObjectUtils.isEmpty(consumerProperties.getExtension().getStartOffset())) {
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
					consumerProperties.getExtension().getStartOffset().name());
		}
		return props;
	}

	/*
	 * TODO: Copied from Kafka binder - refactor to core
	 */
	private Map<String, Object> createProducerConfigs(
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {

		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				ByteArraySerializer.class);
		props.put(ProducerConfig.ACKS_CONFIG,
				String.valueOf(this.configurationProperties.getRequiredAcks()));
		Map<String, Object> mergedConfig = this.configurationProperties
				.mergedProducerConfiguration();
		if (!ObjectUtils.isEmpty(mergedConfig)) {
			props.putAll(mergedConfig);
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.configurationProperties.getKafkaConnectionString());
		}
		final KafkaProducerProperties kafkaProducerProperties = producerProperties.getExtension();
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BATCH_SIZE_CONFIG))) {
			props.put(ProducerConfig.BATCH_SIZE_CONFIG,
					String.valueOf(kafkaProducerProperties.getBufferSize()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.LINGER_MS_CONFIG))) {
			props.put(ProducerConfig.LINGER_MS_CONFIG,
					String.valueOf(kafkaProducerProperties.getBatchTimeout()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG))) {
			props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
					kafkaProducerProperties.getCompressionType().toString());
		}
		Map<String, String> configs = producerProperties.getExtension().getConfiguration();
		Assert.state(!configs.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " cannot be overridden at the binding level; "
						+ "use multiple binders instead");
		if (!ObjectUtils.isEmpty(configs)) {
			props.putAll(configs);
		}
		if (!ObjectUtils.isEmpty(kafkaProducerProperties.getConfiguration())) {
			props.putAll(kafkaProducerProperties.getConfiguration());
		}
		return props;
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
}

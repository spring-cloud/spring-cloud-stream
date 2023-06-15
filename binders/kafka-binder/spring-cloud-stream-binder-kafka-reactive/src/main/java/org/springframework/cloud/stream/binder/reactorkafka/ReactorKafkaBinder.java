/*
 * Copyright 2021-2023 the original author or authors.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import org.springframework.beans.factory.ObjectProvider;
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
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.KafkaMessageHeaders;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Gary Russell
 * @author Byungjun You
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

	private ReceiverOptionsCustomizer<Object, Object> receiverOptionsCustomizer = (name, opts) -> opts;

	private SenderOptionsCustomizer<Object, Object> senderOptionsCustomizer = (name, opts) -> opts;

	private final Map<String, TopicInformation> topicsInUse = new ConcurrentHashMap<>();

	private final Map<String, MessageProducerSupport> messageProducers = new ConcurrentHashMap<>();

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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void receiverOptionsCustomizers(ObjectProvider<ReceiverOptionsCustomizer> customizers) {
		if (customizers.getIfUnique() != null) {
			this.receiverOptionsCustomizer = customizers.getIfUnique();
		}
		else {
			List<ReceiverOptionsCustomizer> list = customizers.orderedStream().toList();
			ReceiverOptionsCustomizer customizer = (name, opts) -> {
				ReceiverOptions<Object, Object> last = null;
				for (ReceiverOptionsCustomizer cust: list) {
					last = (ReceiverOptions<Object, Object>) cust.apply(name, opts);
				}
				return last;
			};
			if (!list.isEmpty()) {
				this.receiverOptionsCustomizer = customizer;
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void senderOptionsCustomizers(ObjectProvider<SenderOptionsCustomizer> customizers) {
		if (customizers.getIfUnique() != null) {
			this.senderOptionsCustomizer = customizers.getIfUnique();
		}
		else {
			List<SenderOptionsCustomizer> list = customizers.orderedStream().toList();
			SenderOptionsCustomizer customizer = (name, opts) -> {
				SenderOptions<Object, Object> last = null;
				for (SenderOptionsCustomizer cust: list) {
					last = (SenderOptions<Object, Object>) cust.apply(name, opts);
				}
				return last;
			};
			if (!list.isEmpty()) {
				this.senderOptionsCustomizer = customizer;
			}
		}
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
		Map<String, Object> props = BindingUtils.createProducerConfigs(producerProperties,
			this.configurationProperties);
		DefaultKafkaProducerFactory<byte[], byte[]> producerFactory = new DefaultKafkaProducerFactory<>(props);
		Collection<PartitionInfo> partitions = provisioningProvider.getPartitionsForTopic(
			producerProperties.getPartitionCount(), false, () -> {
				Producer<byte[], byte[]> producer = producerFactory.createProducer();
				List<PartitionInfo> partitionsFor = producer
					.partitionsFor(destination.getName());
				producer.close();
				return partitionsFor;
			}, destination.getName());

		this.topicsInUse.put(destination.getName(),
			new TopicInformation(null, partitions, false));

		SenderOptions<Object, Object> opts = this.senderOptionsCustomizer.apply(producerProperties.getBindingName(),
				SenderOptions.create(configs));
		// TODO bean for converter; MCB doesn't use one on the producer side.
		RecordMessageConverter converter = new MessagingMessageConverter();
		AbstractApplicationContext applicationContext = getApplicationContext();
		FluxMessageChannel resultChannel = null;
		String channelName = producerProperties.getExtension().getRecordMetadataChannel();
		if (channelName != null && applicationContext.containsBean(channelName)) {
			resultChannel = applicationContext.getBean(channelName, FluxMessageChannel.class);
		}
		return new ReactorMessageHandler(opts, converter, destination.getName(), resultChannel);
	}

	// TODO: Refactor to provide in a common area since KafkaMessageChannelBinder also provides this.
	public void processTopic(final String group, final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
							final ConsumerFactory<?, ?> consumerFactory, int partitionCount,
							boolean usingPatterns, boolean groupManagement, String topic) {
		Collection<PartitionInfo> listenedPartitions;
		Collection<PartitionInfo> allPartitions = usingPatterns ? Collections.emptyList()
			: getPartitionInfo(topic, extendedConsumerProperties, consumerFactory,
			partitionCount);

		if (groupManagement || extendedConsumerProperties.getInstanceCount() == 1) {
			listenedPartitions = allPartitions;
		}
		else {
			listenedPartitions = new ArrayList<>();
			for (PartitionInfo partition : allPartitions) {
				// divide partitions across modules
				if ((partition.partition() % extendedConsumerProperties
					.getInstanceCount()) == extendedConsumerProperties
					.getInstanceIndex()) {
					listenedPartitions.add(partition);
				}
			}
		}
		this.topicsInUse.put(topic,
			new TopicInformation(group, listenedPartitions, usingPatterns));
	}

	private Collection<PartitionInfo> getPartitionInfo(String topic,
													final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
													final ConsumerFactory<?, ?> consumerFactory, int partitionCount) {
		return provisioningProvider.getPartitionsForTopic(partitionCount,
			extendedConsumerProperties.getExtension().isAutoRebalanceEnabled(),
			() -> {
				try (Consumer<?, ?> consumer = consumerFactory.createConsumer()) {
					return consumer.partitionsFor(topic);
				}
			}, topic);
	}

	Map<String, TopicInformation> getTopicsInUse() {
		return this.topicsInUse;
	}


	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {

		boolean anonymous = !StringUtils.hasText(group);
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID() : group;
		Map<String, Object> configs = BindingUtils.createConsumerConfigs(anonymous, consumerGroup, properties,
				this.configurationProperties);

		String destinations = destination.getName();
		if (this.consumerConfigCustomizer != null) {
			this.consumerConfigCustomizer.configure(configs, properties.getBindingName(), destinations);
		}

		MessageConverter converter = BindingUtils.getConsumerMessageConverter(getApplicationContext(), properties,
				this.configurationProperties);
		Assert.isInstanceOf(RecordMessageConverter.class, converter);
		/*
		 *  No need to check multiplex here because, if false, the topics are bound one-at-a-time;
		 *  it is still required by the provisioner, however.
		 */
		List<String> destList = Arrays.stream(StringUtils.commaDelimitedListToStringArray(destinations))
				.map(String::trim)
				.toList();
		ReceiverOptions<Object, Object> opts = ReceiverOptions.create(configs)
			.addAssignListener(parts -> logger.info("Assigned: " + parts));
		if (properties.getExtension().isDestinationIsPattern()) {
			opts = opts.subscription(Pattern.compile(destinations));
		}
		else {
			opts = opts.subscription(destList);
		}
		opts = this.receiverOptionsCustomizer.apply(properties.getBindingName(), opts);
		ReceiverOptions<Object, Object> finalOpts = opts;

		Map<String, Object> props = BindingUtils.createConsumerConfigs(anonymous, consumerGroup, properties,
			this.configurationProperties);

		DefaultKafkaConsumerFactory<Object, Object> factory = new DefaultKafkaConsumerFactory<>(props);
		int partitionCount = properties.getInstanceCount() * properties.getConcurrency();
		boolean groupManagement = properties.getExtension().isAutoRebalanceEnabled();
		processTopic(consumerGroup, properties, factory, partitionCount, properties.getExtension().isDestinationIsPattern(),
			groupManagement, destination.getName());

		class ReactorMessageProducer extends MessageProducerSupport {

			private final List<KafkaReceiver<Object, Object>> receivers = new ArrayList<>();

			ReactorMessageProducer() {
				for (int i = 0; i < properties.getConcurrency(); i++) {
					this.receivers.add(KafkaReceiver.create(finalOpts));
				}
			}

			@SuppressWarnings("unchecked")
			@Override
			protected void doStart() {
				List<Flux<Message<Object>>> fluxes = new ArrayList<>();
				int concurrency = properties.getConcurrency();
				boolean autoCommit = properties.getExtension().isReactiveAutoCommit();
				boolean atMostOnce = properties.getExtension().isReactiveAtMostOnce();
				Assert.state(!(autoCommit && atMostOnce),
						"Cannot set both reactiveAutoCommit and reactiveAtMostOnce");
				for (int i = 0; i < concurrency; i++) {
					Flux<? extends ConsumerRecord<Object, Object>> receive = null;
					KafkaReceiver<Object, Object> kafkaReceiver = this.receivers.get(i);
					if (atMostOnce) {
						receive = kafkaReceiver
								.receiveAtmostOnce();
					}
					else if (!autoCommit) {
						receive = kafkaReceiver
								.receive();
					}
					if (autoCommit) {
						fluxes.add(kafkaReceiver
								.receiveAutoAck()
								.map(inner -> new GenericMessage<>(inner)));
					}
					else {
						fluxes.add(receive
								.map(record -> {
									Message<Object> message = (Message<Object>) ((RecordMessageConverter) converter)
										.toMessage(record, null, null, null);
									return addAckHeaderIfNeeded(atMostOnce, record, message);
								}));
					}
				}
				if (concurrency == 1) {
					subscribeToPublisher(fluxes.get(0));
				}
				else {
					subscribeToPublisher(Flux.merge(fluxes));
				}
			}

			private Message<Object> addAckHeaderIfNeeded(boolean autoCommit, ConsumerRecord<Object, Object> record,
					Message<Object> message) {

				if (!autoCommit) {
					if (message.getHeaders() instanceof KafkaMessageHeaders headers) {
						headers.getRawHeaders().put(KafkaHeaders.ACKNOWLEDGMENT,
								((ReceiverRecord<Object, Object>) record).receiverOffset());
					}
					else {
						message = MessageBuilder.fromMessage(message)
								.setHeader(KafkaHeaders.ACKNOWLEDGMENT,
										((ReceiverRecord<Object, Object>) record).receiverOffset())
								.build();
					}
				}
				return message;
			}

		}
		ReactorMessageProducer reactorMessageProducer = new ReactorMessageProducer();
		this.messageProducers.put(consumerGroup, reactorMessageProducer);
		return reactorMessageProducer;
	}

	public Map<String, MessageProducerSupport> getMessageProducers() {
		return this.messageProducers;
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

		@Nullable
		private final FluxMessageChannel results;

		private volatile KafkaSender<Object, Object> sender;

		private volatile boolean running;

		ReactorMessageHandler(SenderOptions<Object, Object> opts, RecordMessageConverter converter,
				String topic, @Nullable FluxMessageChannel results) {

			this.senderOptions = opts;
			this.converter = converter;
			this.topic = topic;
			this.results = results;
		}

		@Override
		protected void handleMessageInternal(Message<?> message) {
			if (this.sender != null) {
				Object correlation = message.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
				if (correlation == null) {
					correlation = UUID.randomUUID();
				}
				@SuppressWarnings("unchecked")
				SenderRecord<Object, Object, Object> sr = SenderRecord.create(
						(ProducerRecord<Object, Object>) converter.fromMessage(message, topic), correlation);
				Flux<SenderResult<Object>> result = sender.send(Flux.just(sr));
				result.subscribe(res -> {
					if (this.results != null) {
						this.results.send(MessageBuilder.withPayload(res)
							.copyHeaders(message.getHeaders())
							.build());
					}
				});
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

	/**
	 * Inner class to capture topic details.
	 *
	 * TODO: Refactor this as KafkaMessageChannelBinder also provides this same info
	 */
	static class TopicInformation {

		private final String consumerGroup;

		private final Collection<PartitionInfo> partitionInfos;

		private final boolean isTopicPattern;

		TopicInformation(String consumerGroup, Collection<PartitionInfo> partitionInfos,
						boolean isTopicPattern) {
			this.consumerGroup = consumerGroup;
			this.partitionInfos = partitionInfos;
			this.isTopicPattern = isTopicPattern;
		}

		String getConsumerGroup() {
			return this.consumerGroup;
		}

		boolean isConsumerTopic() {
			return this.consumerGroup != null;
		}

		boolean isTopicPattern() {
			return this.isTopicPattern;
		}

		Collection<PartitionInfo> getPartitionInfos() {
			return this.partitionInfos;
		}

	}

}

/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.integration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
public class StreamToGlobalKTableJoinIntegrationTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"enriched-order");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	private static Consumer<Long, EnrichedOrder> consumer;

	@Test
	public void testStreamToGlobalKTable() throws Exception {
		SpringApplication app = new SpringApplication(
				StreamToGlobalKTableJoinIntegrationTests.OrderEnricherApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.destination=orders",
				"--spring.cloud.stream.bindings.input-x.destination=customers",
				"--spring.cloud.stream.bindings.input-y.destination=products",
				"--spring.cloud.stream.bindings.output.destination=enriched-order",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=10000",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.applicationId"
						+ "=StreamToGlobalKTableJoinIntegrationTests-abc",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.topic.properties.cleanup.policy=compact",
				"--spring.cloud.stream.kafka.streams.bindings.input-x.consumer.topic.properties.cleanup.policy=compact",
				"--spring.cloud.stream.kafka.streams.bindings.input-y.consumer.topic.properties.cleanup.policy=compact",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString());
		try {
			// Testing certain ancillary configuration of GlobalKTable around topics creation.
			// See this issue: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/687

			BinderFactory binderFactory = context.getBeanFactory()
					.getBean(BinderFactory.class);

			Binder<KStream, ? extends ConsumerProperties, ? extends ProducerProperties> kStreamBinder = binderFactory
					.getBinder("kstream", KStream.class);

			KafkaStreamsConsumerProperties input = (KafkaStreamsConsumerProperties) ((ExtendedPropertiesBinder) kStreamBinder)
					.getExtendedConsumerProperties("input");
			String cleanupPolicy = input.getTopic().getProperties().get("cleanup.policy");

			assertThat(cleanupPolicy).isEqualTo("compact");

			Binder<GlobalKTable, ? extends ConsumerProperties, ? extends ProducerProperties> globalKTableBinder = binderFactory
					.getBinder("globalktable", GlobalKTable.class);

			KafkaStreamsConsumerProperties inputX = (KafkaStreamsConsumerProperties) ((ExtendedPropertiesBinder) globalKTableBinder)
					.getExtendedConsumerProperties("input-x");
			String cleanupPolicyX = inputX.getTopic().getProperties().get("cleanup.policy");

			assertThat(cleanupPolicyX).isEqualTo("compact");

			KafkaStreamsConsumerProperties inputY = (KafkaStreamsConsumerProperties) ((ExtendedPropertiesBinder) globalKTableBinder)
					.getExtendedConsumerProperties("input-y");
			String cleanupPolicyY = inputY.getTopic().getProperties().get("cleanup.policy");

			assertThat(cleanupPolicyY).isEqualTo("compact");

			Map<String, Object> senderPropsCustomer = KafkaTestUtils
					.producerProps(embeddedKafka);
			senderPropsCustomer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					LongSerializer.class);
			senderPropsCustomer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					JsonSerializer.class);

			DefaultKafkaProducerFactory<Long, Customer> pfCustomer = new DefaultKafkaProducerFactory<>(
					senderPropsCustomer);
			KafkaTemplate<Long, Customer> template = new KafkaTemplate<>(pfCustomer,
					true);
			template.setDefaultTopic("customers");
			for (long i = 0; i < 5; i++) {
				final Customer customer = new Customer();
				customer.setName("customer-" + i);
				template.sendDefault(i, customer);
			}

			Map<String, Object> senderPropsProduct = KafkaTestUtils
					.producerProps(embeddedKafka);
			senderPropsProduct.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					LongSerializer.class);
			senderPropsProduct.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					JsonSerializer.class);

			DefaultKafkaProducerFactory<Long, Product> pfProduct = new DefaultKafkaProducerFactory<>(
					senderPropsProduct);
			KafkaTemplate<Long, Product> productTemplate = new KafkaTemplate<>(pfProduct,
					true);
			productTemplate.setDefaultTopic("products");

			for (long i = 0; i < 5; i++) {
				final Product product = new Product();
				product.setName("product-" + i);
				productTemplate.sendDefault(i, product);
			}

			Map<String, Object> senderPropsOrder = KafkaTestUtils
					.producerProps(embeddedKafka);
			senderPropsOrder.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					LongSerializer.class);
			senderPropsOrder.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					JsonSerializer.class);

			DefaultKafkaProducerFactory<Long, Order> pfOrder = new DefaultKafkaProducerFactory<>(
					senderPropsOrder);
			KafkaTemplate<Long, Order> orderTemplate = new KafkaTemplate<>(pfOrder, true);
			orderTemplate.setDefaultTopic("orders");

			for (long i = 0; i < 5; i++) {
				final Order order = new Order();
				order.setCustomerId(i);
				order.setProductId(i);
				orderTemplate.sendDefault(i, order);
			}

			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group",
					"false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					LongDeserializer.class);
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					JsonDeserializer.class);
			consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE,
					"org.springframework.cloud.stream.binder.kafka.streams.integration."
							+ "StreamToGlobalKTableJoinIntegrationTests.EnrichedOrder");
			DefaultKafkaConsumerFactory<Long, EnrichedOrder> cf = new DefaultKafkaConsumerFactory<>(
					consumerProps);

			consumer = cf.createConsumer();
			embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "enriched-order");

			int count = 0;
			long start = System.currentTimeMillis();
			List<KeyValue<Long, EnrichedOrder>> enrichedOrders = new ArrayList<>();
			do {
				ConsumerRecords<Long, EnrichedOrder> records = KafkaTestUtils
						.getRecords(consumer);
				count = count + records.count();
				for (ConsumerRecord<Long, EnrichedOrder> record : records) {
					enrichedOrders.add(new KeyValue<>(record.key(), record.value()));
				}
			}
			while (count < 5 && (System.currentTimeMillis() - start) < 30000);

			assertThat(count == 5).isTrue();
			assertThat(enrichedOrders.size() == 5).isTrue();

			enrichedOrders.sort(Comparator.comparing(o -> o.key));

			for (int i = 0; i < 5; i++) {
				KeyValue<Long, EnrichedOrder> enrichedOrderKeyValue = enrichedOrders
						.get(i);
				assertThat(enrichedOrderKeyValue.key == i).isTrue();
				EnrichedOrder enrichedOrder = enrichedOrderKeyValue.value;
				assertThat(enrichedOrder.getOrder().customerId == i).isTrue();
				assertThat(enrichedOrder.getOrder().productId == i).isTrue();
				assertThat(enrichedOrder.getCustomer().name.equals("customer-" + i))
						.isTrue();
				assertThat(enrichedOrder.getProduct().name.equals("product-" + i))
						.isTrue();
			}
			pfCustomer.destroy();
			pfProduct.destroy();
			pfOrder.destroy();
			consumer.close();
		}
		finally {
			context.close();
		}
	}

	interface CustomGlobalKTableProcessor extends KafkaStreamsProcessor {

		@Input("input-x")
		GlobalKTable<?, ?> inputX();

		@Input("input-y")
		GlobalKTable<?, ?> inputY();

	}

	@EnableBinding(CustomGlobalKTableProcessor.class)
	@EnableAutoConfiguration
	public static class OrderEnricherApplication {

		@StreamListener
		@SendTo("output")
		public KStream<Long, EnrichedOrder> process(
				@Input("input") KStream<Long, Order> ordersStream,
				@Input("input-x") GlobalKTable<Long, Customer> customers,
				@Input("input-y") GlobalKTable<Long, Product> products) {

			KStream<Long, CustomerOrder> customerOrdersStream = ordersStream.join(
					customers, (orderId, order) -> order.getCustomerId(),
					(order, customer) -> new CustomerOrder(customer, order));

			return customerOrdersStream.join(products,
					(orderId, customerOrder) -> customerOrder.productId(),
					(customerOrder, product) -> {
						EnrichedOrder enrichedOrder = new EnrichedOrder();
						enrichedOrder.setProduct(product);
						enrichedOrder.setCustomer(customerOrder.customer);
						enrichedOrder.setOrder(customerOrder.order);
						return enrichedOrder;
					});
		}

	}

	static class Order {

		long customerId;

		long productId;

		public long getCustomerId() {
			return customerId;
		}

		public void setCustomerId(long customerId) {
			this.customerId = customerId;
		}

		public long getProductId() {
			return productId;
		}

		public void setProductId(long productId) {
			this.productId = productId;
		}

	}

	static class Customer {

		String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

	static class Product {

		String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

	static class EnrichedOrder {

		Product product;

		Customer customer;

		Order order;

		public Product getProduct() {
			return product;
		}

		public void setProduct(Product product) {
			this.product = product;
		}

		public Customer getCustomer() {
			return customer;
		}

		public void setCustomer(Customer customer) {
			this.customer = customer;
		}

		public Order getOrder() {
			return order;
		}

		public void setOrder(Order order) {
			this.order = order;
		}

	}

	private static class CustomerOrder {

		private final Customer customer;

		private final Order order;

		CustomerOrder(final Customer customer, final Order order) {
			this.customer = customer;
			this.order = order;
		}

		long productId() {
			return order.getProductId();
		}

	}
}

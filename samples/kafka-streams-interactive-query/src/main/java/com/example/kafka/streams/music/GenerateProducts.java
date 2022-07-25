/*
 * Copyright 2018-2022 the original author or authors.
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

package com.example.kafka.streams.music;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.example.kafka.streams.music.KafkaStreamsInteractiveQueryApplication.InteractiveProductCountApplication;
import com.example.kafka.streams.music.KafkaStreamsInteractiveQueryApplication.Product;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;

public class GenerateProducts {

	private static final Logger LOG = LoggerFactory.getLogger(GenerateProducts.class);

	public static void main(String... args) throws Exception {

		Serde<Product> productSerde = new JsonSerde<>(Product.class);

		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, productSerde.serializer().getClass());

		DefaultKafkaProducerFactory<Integer, Product> producerFactory = new DefaultKafkaProducerFactory<>(props);
		KafkaTemplate<Integer, Product> template = new KafkaTemplate<>(producerFactory, true);
		template.setDefaultTopic(InteractiveProductCountApplication.PRODUCT_TOPIC);

		final List<Product> products = Arrays.asList(
				new Product(123),
				new Product(124),
				new Product(125),
				new Product(126),
				new Product(127));

		final Random random = new Random();

		// send a product event every 100 milliseconds
		while (true) {
			final Product product = products.get(random.nextInt(products.size()));
			LOG.debug("Writing product event for productId = {}", product.id());
			template.sendDefault(product.id(), product);
			Thread.sleep(1000L);
		}

	}
}

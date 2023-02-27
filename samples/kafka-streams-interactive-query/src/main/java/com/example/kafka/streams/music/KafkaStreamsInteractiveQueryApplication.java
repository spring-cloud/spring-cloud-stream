/*
 * Copyright 2017-2023 the original author or authors.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

@SpringBootApplication(proxyBeanMethods = false)
class KafkaStreamsInteractiveQueryApplication {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsInteractiveQueryApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsInteractiveQueryApplication.class, args);
	}

	@EnableConfigurationProperties(ProductTrackerProperties.class)
	static class InteractiveProductCountApplication {

		static final String PRODUCT_TOPIC = "product-events";
		static final String PRODUCT_COUNT_STORE_NAME = "product-count-store";
		static final String TOP_TWO_PRODUCTS_STORE_NAME = "top-two-products-store";
		static final String TOP_TWO_PRODUCTS_KEY = "top-two";

		@Value("${spring.cloud.stream.kafka.streams.binder.configuration.state.dir:UNKNOWN}")
		private String stateStoreDir;

		private final ProductTrackerProperties productTrackerProperties;

		InteractiveProductCountApplication(ProductTrackerProperties productTrackerProperties) {
			this.productTrackerProperties = productTrackerProperties;
		}

		@PostConstruct
		void showStateStoreInfo() {
			LOG.info("Using KafkaStreams state.dir = {}", this.stateStoreDir);
		}

		@Bean
		public Consumer<KStream<Object, Product>> process() {
			return (input) -> {
				// Accept product events that are configured to be tracked
				final KStream<Integer, Product> productsByProductId = input
						.filter((key, product) -> productTrackerProperties.getProductIds().contains(product.id()))
						.map((key, value) -> new KeyValue<>(value.id, value));

				Serde<Product> productSerde = new JsonSerde<>(Product.class);
				Serde<ProductCount> productCountSerde = new JsonSerde<>(ProductCount.class);
				Serde<TopTwoProducts> topTwoSerde = new JsonSerde<>(TopTwoProducts.class).noTypeInfo();

				// Create a state store to track product counts
				final KTable<Product, Long> productCounts = productsByProductId
						.groupBy((productId, product) -> product, Grouped.with(productSerde, productSerde))
						.count(Materialized.<Product, Long, KeyValueStore<Bytes, byte[]>>as(PRODUCT_COUNT_STORE_NAME)
								.withKeySerde(productSerde)
								.withValueSerde(Serdes.Long()));

				// Compute the top two products and store updated result in 'top-two-products-store'
				productCounts.groupBy((product, count) ->
								KeyValue.pair(TOP_TWO_PRODUCTS_KEY, new ProductCount(product.id(), count)),
								Grouped.with(Serdes.String(), productCountSerde))
						.aggregate(
								TopTwoProducts::new,
								(aggKey, value, aggregate) -> aggregate.add(value),
								(aggKey, value, aggregate) -> aggregate.remove(value),
								Materialized.<String, TopTwoProducts, KeyValueStore<Bytes, byte[]>>as(TOP_TWO_PRODUCTS_STORE_NAME)
										.withKeySerde(Serdes.String())
										.withValueSerde(topTwoSerde)); //new TopTwoProductsSerde()));
			};
		}
	}

	record Product(Integer id) { }

	record ProductCount(Integer productId, Long count) { }

	@ConfigurationProperties(prefix = "app.product.tracker")
	static class ProductTrackerProperties {

		private Set<Integer> productIds = new HashSet<>();

		public Set<Integer> getProductIds() {
			return productIds;
		}

		public void setProductIds(Set<Integer> productIds) {
			this.productIds = productIds;
		}
	}

	/**
	 * Used in aggregations to keep track of the Top two products
	 */
	static class TopTwoProducts {

		@JsonIgnore
		private final Map<Integer, ProductCount> currentProducts = new HashMap<>();

		@JsonIgnore
		private final TreeSet<ProductCount> topTwo = new TreeSet<>((o1, o2) -> {
			final int result = Long.compare(o2.count(), o1.count());
			if (result != 0) {
				return result;
			}
			return Integer.compare(o1.productId(), o2.productId());
		});

		public TopTwoProducts add(final ProductCount productCount) {
			if (currentProducts.containsKey(productCount.productId())) {
				topTwo.remove(currentProducts.remove(productCount.productId()));
			}
			topTwo.add(productCount);
			currentProducts.put(productCount.productId(), productCount);
			if (topTwo.size() > 2) {
				final ProductCount last = topTwo.last();
				currentProducts.remove(last.productId());
				topTwo.remove(last);
			}
			return this;
		}

		public TopTwoProducts remove(final ProductCount value) {
			topTwo.remove(value);
			currentProducts.remove(value.productId());
			return this;
		}

		@JsonGetter
		public List<ProductCount> products() {
			return topTwo.stream().toList();
		}

		@JsonSetter
		@SuppressWarnings("unused")
		public void setProducts(List<ProductCount> products) {
			products.forEach(this::add);
		}
	}
}

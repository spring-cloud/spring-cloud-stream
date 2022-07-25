package com.example.kafka.streams.music;

import java.util.List;
import java.util.Optional;

import com.example.kafka.streams.music.KafkaStreamsInteractiveQueryApplication.Product;
import com.example.kafka.streams.music.KafkaStreamsInteractiveQueryApplication.ProductCount;
import com.example.kafka.streams.music.KafkaStreamsInteractiveQueryApplication.TopTwoProducts;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import static com.example.kafka.streams.music.KafkaStreamsInteractiveQueryApplication.InteractiveProductCountApplication.PRODUCT_COUNT_STORE_NAME;
import static com.example.kafka.streams.music.KafkaStreamsInteractiveQueryApplication.InteractiveProductCountApplication.TOP_TWO_PRODUCTS_KEY;
import static com.example.kafka.streams.music.KafkaStreamsInteractiveQueryApplication.InteractiveProductCountApplication.TOP_TWO_PRODUCTS_STORE_NAME;

@RestController
class ProductQueryController {

	private static final Logger LOG = LoggerFactory.getLogger(ProductQueryController.class);

	private final InteractiveQueryService queryService;

	ProductQueryController(InteractiveQueryService queryService) {
		this.queryService = queryService;
	}

	@GetMapping("/product/{id}")
	public ResponseEntity<ProductCount> productCount(@PathVariable("id") Integer productId) {
		JsonSerializer<Product> productSerializer = new JsonSerde<Product>().serializer();
		HostInfo hostInfo = queryService.getHostInfo(PRODUCT_COUNT_STORE_NAME, new Product(productId), productSerializer);
		if (queryService.getCurrentHostInfo().equals(hostInfo)) {
			LOG.info("Product count for productId: {} served from same host: {}", productId, hostInfo);
			ReadOnlyKeyValueStore<Product, Long> productCountStore =
					queryService.getQueryableStore(PRODUCT_COUNT_STORE_NAME, QueryableStoreTypes.keyValueStore());
			Long count = productCountStore.get(new Product(productId));
			if (count == null) {
				return ResponseEntity.notFound().build();
			}
			return ResponseEntity.of(Optional.of(new ProductCount(productId, count)));
		}
		LOG.info("Product count for productId: {} served from different host: {}", productId, hostInfo);
		RestTemplate restTemplate = new RestTemplate();
		return restTemplate.getForEntity(
				String.format("http://%s:%d/product/%d", hostInfo.host(), hostInfo.port(), productId), ProductCount.class);
	}

	@GetMapping("/product/top-two")
	public ResponseEntity<List<ProductCount>> topTwo() {
		HostInfo hostInfo = queryService.getHostInfo(TOP_TWO_PRODUCTS_STORE_NAME, TOP_TWO_PRODUCTS_KEY, new StringSerializer());
		if (queryService.getCurrentHostInfo().equals(hostInfo)) {
			LOG.info("Top two products served from same host: {}", hostInfo);
			ReadOnlyKeyValueStore<String, TopTwoProducts> topTwoProductsStore =
					queryService.getQueryableStore(TOP_TWO_PRODUCTS_STORE_NAME, QueryableStoreTypes.keyValueStore());
			TopTwoProducts topTwo = topTwoProductsStore.get(TOP_TWO_PRODUCTS_KEY);
			if (topTwo == null) {
				return ResponseEntity.notFound().build();
			}
			return ResponseEntity.of(Optional.of(topTwo.products()));
		}
		LOG.info("Top two products served from different host: {}", hostInfo);
		RestTemplate restTemplate = new RestTemplate();
		return restTemplate.exchange(String.format("http://%s:%d/product/top-two", hostInfo.host(), hostInfo.port()),
				HttpMethod.GET,
				null,
				new ParameterizedTypeReference<>() {});
	}

}

/*
 * Copyright 2012-2019 the original author or authors.
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

package org.springframework.cloud.bus.jackson;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(properties = "spring.jackson.serialization.WRITE_DATES_AS_TIMESTAMPS:true",
		webEnvironment = RANDOM_PORT)
@DirtiesContext
public class BusJacksonIntegrationTests {

	@LocalServerPort
	private int port;

	@Autowired
	private TestRestTemplate rest;

	@Autowired
	private BusJacksonMessageConverter converter;

	@Test
	@SuppressWarnings("unchecked")
	public void testCustomEventSerializes() {
		assertThat(this.converter.isMapperCreated()).isFalse();

		// set by configuration
		assertThat(this.converter.getMapper().getSerializationConfig()
				.isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)).isTrue();

		Map map = this.rest.getForObject("http://localhost:" + this.port + "/date", Map.class);
		assertThat(map).containsOnlyKeys("date");
		assertThat(map.get("date")).isInstanceOf(Long.class);

		this.rest.put("http://localhost:" + this.port + "/names" + "/foo", null);
		this.rest.put("http://localhost:" + this.port + "/names" + "/bar", null);

		ResponseEntity<List> response = this.rest.getForEntity("http://localhost:" + this.port + "/names", List.class);
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(response.getBody()).contains("foo", "bar");
	}

	public static class NameEvent extends RemoteApplicationEvent {

		private String name;

		protected NameEvent() {
		}

		public NameEvent(Object source, String originService, String name) {
			super(source, originService);
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

	@RestController
	@EnableAutoConfiguration
	@SpringBootConfiguration
	@RemoteApplicationEventScan
	protected static class Config {

		final private Set<String> names = ConcurrentHashMap.newKeySet();

		@Autowired
		private ServiceMatcher busServiceMatcher;

		@Autowired
		private ApplicationEventPublisher publisher;

		@GetMapping("/names")
		public Collection<String> names() {
			return this.names;
		}

		@PutMapping("/names/{name}")
		public void sayName(@PathVariable String name) {
			this.names.add(name);
			this.publisher.publishEvent(new NameEvent(this, this.busServiceMatcher.getBusId(), name));
		}

		@GetMapping("/date")
		public Map<String, Object> testTimeJsonSerialization() {
			Map<String, Object> map = new HashMap<>();
			map.put("date", new Date());
			return map;
		}

		@EventListener
		public void handleNameSaid(NameEvent event) {
			this.names.add(event.getName());
		}

	}

}

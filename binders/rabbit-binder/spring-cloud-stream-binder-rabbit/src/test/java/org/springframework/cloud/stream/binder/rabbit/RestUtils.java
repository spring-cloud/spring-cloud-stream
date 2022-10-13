/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriUtils;

/**
 * @author Gary Russell
 * @since 4.0
 *
 */
public final class RestUtils {

	private RestUtils() {
	}

	public static List<Map<String, Object>> getBindingsBySource(WebClient client, URI uri, String vhost, String name) {
		String exchange = "".equals(name) ? "amq.default" : name;
		URI getUri = uri
				.resolve("/api/exchanges/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/"
						+ UriUtils.encodePathSegment(exchange, StandardCharsets.UTF_8) + "/bindings/source");
		return client.get()
				.uri(getUri)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<List<Map<String, Object>>>() {
				})
				.block(Duration.ofSeconds(10));
	}

	public static Map<String, Object> getExchange(WebClient client, URI uri, String vhost, String name) {
		String exchange = "".equals(name) ? "amq.default" : name;
		URI getUri = uri
				.resolve("/api/exchanges/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/"
						+ UriUtils.encodePathSegment(exchange, StandardCharsets.UTF_8));
		return client.get()
				.uri(getUri)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
				})
				.block(Duration.ofSeconds(10));
	}

	public static Map<String, Object> getQueue(WebClient client, URI uri, String vhost, String name) {
		URI getUri = uri
				.resolve("/api/queues/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/"
						+ UriUtils.encodePathSegment(name, StandardCharsets.UTF_8));
		return client.get()
				.uri(getUri)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
				})
				.block(Duration.ofSeconds(10));
	}

}

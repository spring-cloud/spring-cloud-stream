/*
 * Copyright 2015-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.admin;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.BindingCleaner;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriUtils;

/**
 * Implementation of {@link org.springframework.cloud.stream.binder.BindingCleaner} for
 * the {@code RabbitBinder}.
 *
 * @author Gary Russell
 * @author David Turanski
 * @since 1.2
 */
public class RabbitBindingCleaner implements BindingCleaner {

	private static final Log logger = LogFactory.getLog(RabbitBindingCleaner.class);

	private static final String PREFIX_DELIMITER = ".";

	/**
	 * Binder prefix.
	 */
	public static final String BINDER_PREFIX = "binder" + PREFIX_DELIMITER;

	@Override
	public Map<String, List<String>> clean(String entity, boolean isJob) {
		return clean("http://localhost:15672/api", "guest", "guest", "/", BINDER_PREFIX,
				entity, isJob);
	}

	public Map<String, List<String>> clean(String adminUri, String user, String pw,
			String vhost, String binderPrefix, String entity, boolean isJob) {

		try {
			WebClient client = WebClient.builder()
					.filter(ExchangeFilterFunctions.basicAuthentication(user, pw))
					.build();
			URI uri = new URI(adminUri);
			return doClean(client, uri,
					vhost == null ? "/" : vhost,
					binderPrefix == null ? BINDER_PREFIX : binderPrefix, entity, isJob);
		}
		catch (URISyntaxException e) {
			throw new RabbitAdminException("Couldn't create a Client", e);
		}
	}

	private Map<String, List<String>> doClean(WebClient client,
			URI uri, String vhost, String binderPrefix, String entity, boolean isJob) {

		LinkedList<String> removedQueues = isJob ? null
				: findStreamQueues(client, uri, vhost, binderPrefix, entity);
		List<String> removedExchanges = findExchanges(client, uri, vhost, binderPrefix, entity);
		// Delete the queues in reverse order to enable re-running after a partial
		// success.
		// The queue search above starts with 0 and terminates on a not found.
		if (removedQueues != null) {
			removedQueues.descendingIterator().forEachRemaining(q -> {
				deleteQueue(client, uri, vhost, q);
				if (logger.isDebugEnabled()) {
					logger.debug("deleted queue: " + q);
				}
			});
		}
		Map<String, List<String>> results = new HashMap<>();
		if (removedQueues.size() > 0) {
			results.put("queues", removedQueues);
		}
		// Fanout exchanges for taps
		removedExchanges.forEach(exchange -> {
			deleteExchange(client, uri, vhost, exchange);
			if (logger.isDebugEnabled()) {
				logger.debug("deleted exchange: " + exchange);
			}
		});
		if (removedExchanges.size() > 0) {
			results.put("exchanges", removedExchanges);
		}
		return results;
	}

	private void deleteQueue(WebClient client, URI uri, String vhost, String q) {
		URI deleteURI = uri
				.resolve("/api/queues/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/" + q);
		client.delete()
				.uri(deleteURI)
				.retrieve()
				.toEntity(Void.class)
				.block(Duration.ofSeconds(10));
	}

	private void deleteExchange(WebClient client, URI uri, String vhost, String ex) {
		URI deleteURI = uri
				.resolve("/api/exchanges/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/" + ex);
		client.delete()
				.uri(deleteURI)
				.retrieve()
				.toEntity(Void.class)
				.block(Duration.ofSeconds(10));
	}

	private LinkedList<String> findStreamQueues(WebClient client, URI uri, String vhost, String binderPrefix,
			String stream) {

		String queueNamePrefix = adjustPrefix(AbstractBinder.applyPrefix(binderPrefix, stream));
		List<Map<String, Object>> queues = getQueues(client, uri, vhost);
		return queues.stream()
			.filter(q -> ((String) q.get("name")).startsWith(queueNamePrefix))
			.map(q -> checkNoConsumers(q))
			.collect(Collectors.toCollection(LinkedList::new));
	}

	private List<Map<String, Object>> getQueues(WebClient client, URI uri, String vhost) {
		URI getUri = uri
				.resolve("/api/queues/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/");
		return client.get()
				.uri(getUri)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<List<Map<String, Object>>>() {
				})
				.block(Duration.ofSeconds(10));
	}

	private String adjustPrefix(String prefix) {
		if (prefix.endsWith("*")) {
			return prefix.substring(0, prefix.length() - 1);
		}
		else {
			return prefix + PREFIX_DELIMITER;
		}
	}

	private String checkNoConsumers(Map<String, Object> queue) {
		if ((Integer) queue.get("consumers") != 0) {
			throw new RabbitAdminException("Queue " + queue.get("name") + " is in use");
		}
		return (String) queue.get("name");
	}

	private List<String> findExchanges(WebClient client, URI uri, String vhost, String binderPrefix, String entity) {
		List<Map<String, Object>> exchanges = getExchanges(client, uri, vhost);
		String exchangeNamePrefix = adjustPrefix(AbstractBinder.applyPrefix(binderPrefix, entity));
		List<String> exchangesToRemove = exchanges.stream()
				.filter(e -> ((String) e.get("name")).startsWith(exchangeNamePrefix))
				.map(e -> {
					List<Map<String, Object>> bindingsBySource =
							getBindingsBySource(client, uri, vhost, (String) e.get("name"));
					return Collections.singletonMap((String) e.get("name"), bindingsBySource);
				})
				.map(bindingsMap -> hasNoForeignBindings(bindingsMap, exchangeNamePrefix))
				.collect(Collectors.toList());
		exchangesToRemove.stream()
				.map(exchange -> getExchangeBindingsByDestination(client, uri, vhost, exchange))
				.forEach(bindings -> {
					if (bindings.size() > 0) {
						throw new RabbitAdminException("Cannot delete exchange "
								+ bindings.get(0).get("destination") + "; it is a destination: " + bindings);
					}
				});
		return exchangesToRemove;
	}

	private List<Map<String, Object>> getExchangeBindingsByDestination(WebClient client, URI uri, String vhost,
			String name) {

		String exchange = "".equals(name) ? "amq.default" : name;
		URI getUri = uri
				.resolve("/api/exchanges/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/"
						+ UriUtils.encodePathSegment(exchange, StandardCharsets.UTF_8) + "/bindings/destination");
		return client.get()
				.uri(getUri)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<List<Map<String, Object>>>() {
				})
				.block(Duration.ofSeconds(10));
	}

	private List<Map<String, Object>> getBindingsBySource(WebClient client, URI uri, String vhost, String name) {
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

	private List<Map<String, Object>> getExchanges(WebClient client, URI uri, String vhost) {
		URI getUri = uri
				.resolve("/api/exchanges/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/");
		return client.get()
				.uri(getUri)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<List<Map<String, Object>>>() {
				})
				.block(Duration.ofSeconds(10));
	}

	private String hasNoForeignBindings(Map<String, List<Map<String, Object>>> bindings, String exchangeNamePrefix) {
		Entry<String, List<Map<String, Object>>> next = bindings.entrySet().iterator().next();
		for (Map<String, Object> binding : next.getValue()) {
			if (!"queue".equals(binding.get("destination_type"))
					|| !((String) binding.get("destination")).startsWith(exchangeNamePrefix)) {
				throw new RabbitAdminException("Cannot delete exchange "
						+ next.getKey() + "; it has bindings: " + bindings);
			}
		}
		return next.getKey();
	}

}

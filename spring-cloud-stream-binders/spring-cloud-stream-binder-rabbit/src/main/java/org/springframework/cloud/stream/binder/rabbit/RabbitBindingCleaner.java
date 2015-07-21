/*
 * Copyright 2015 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.rabbit;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.BindingCleaner;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.cloud.stream.binder.BinderUtils;
import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.cloud.stream.binder.RabbitAdminException;
import org.springframework.cloud.stream.binder.RabbitManagementUtils;


/**
 * Implementation of {@link org.springframework.cloud.stream.binder.BindingCleaner} for the {@code RabbitBinder}.
 * @author Gary Russell
 * @author David Turanski
 * @since 1.2
 */
public class RabbitBindingCleaner implements BindingCleaner {

	private final static Logger logger = LoggerFactory.getLogger(RabbitBindingCleaner.class);

	public static final String BINDER_PREFIX = "binder.rabbit.";

	@Override
	public Map<String, List<String>> clean(String entity, boolean isJob) {
		return clean("http://localhost:15672", "guest", "guest", "/", BINDER_PREFIX , entity, isJob);
	}

	public Map<String, List<String>> clean(String adminUri, String user, String pw, String vhost,
			String binderPrefix, String entity, boolean isJob) {
		return doClean(
				adminUri == null ? "http://localhost:15672" : adminUri,
				user == null ? "guest" : user,
				pw == null ? "guest" : pw,
				vhost == null ? "/" : vhost,
				//TODO: Change prefix
				binderPrefix == null ? BINDER_PREFIX  : binderPrefix,
				entity, isJob);
	}

	private Map<String, List<String>> doClean(String adminUri, String user, String pw, String vhost,
			String binderPrefix, String entity, boolean isJob) {
		RestTemplate restTemplate = RabbitManagementUtils.buildRestTemplate(adminUri, user, pw);
		List<String> removedQueues = isJob
				? null
				: findStreamQueues(adminUri, vhost, binderPrefix, entity, restTemplate);
		ExchangeCandidateCallback callback = null;
		if (isJob) {
		}
		else {
			final String tapPrefix = adjustPrefix(MessageChannelBinderSupport.applyPrefix(binderPrefix,
					MessageChannelBinderSupport.applyPubSub(BinderUtils.constructTapPrefix(entity))));
			callback = new ExchangeCandidateCallback() {

				@Override
				public boolean isCandidate(String exchangeName) {
					return exchangeName.startsWith(tapPrefix);
				}
			};
		}
		List<String> removedExchanges = findExchanges(adminUri, vhost, binderPrefix, entity, restTemplate, callback);
		// Delete the queues in reverse order to enable re-running after a partial success.
		// The queue search above starts with 0 and terminates on a not found.
		for (int i = removedQueues.size() - 1; i >= 0; i--) {
			String queueName = removedQueues.get(i);
			URI uri = UriComponentsBuilder.fromUriString(adminUri + "/api")
					.pathSegment("queues", "{vhost}", "{stream}")
					.buildAndExpand(vhost, queueName).encode().toUri();
			restTemplate.delete(uri);
			if (logger.isDebugEnabled()) {
				logger.debug("deleted queue: " + queueName);
			}
		}
		Map<String, List<String>> results = new HashMap<>();
		if (removedQueues.size() > 0) {
			results.put("queues", removedQueues);
		}
		// Fanout exchanges for taps
		for (String exchange : removedExchanges) {
			URI uri = UriComponentsBuilder.fromUriString(adminUri + "/api")
					.pathSegment("exchanges", "{vhost}", "{name}")
					.buildAndExpand(vhost, exchange).encode().toUri();
			restTemplate.delete(uri);
			if (logger.isDebugEnabled()) {
				logger.debug("deleted exchange: " + exchange);
			}
		}
		if (removedExchanges.size() > 0) {
			results.put("exchanges", removedExchanges);
		}
		return results;
	}

	private List<String> findStreamQueues(String adminUri, String vhost, String binderPrefix, String stream,
			RestTemplate restTemplate) {
		String queueNamePrefix = adjustPrefix(MessageChannelBinderSupport.applyPrefix(binderPrefix, stream));
		List<Map<String, Object>> queues = listAllQueues(adminUri, vhost, restTemplate);
		List<String> removedQueues = new ArrayList<>();
		for (Map<String, Object> queue : queues) {
			String queueName = (String) queue.get("name");
			if (queueName.startsWith(queueNamePrefix)) {
				checkNoConsumers(queueName, queue);
				removedQueues.add(queueName);
			}
		}
		return removedQueues;
	}

	private List<Map<String, Object>> listAllQueues(String adminUri, String vhost, RestTemplate restTemplate) {
		URI uri = UriComponentsBuilder.fromUriString(adminUri + "/api")
				.pathSegment("queues", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> queues = restTemplate.getForObject(uri, List.class);
		return queues;
	}

	private String adjustPrefix(String prefix) {
		if (prefix.endsWith("*")) {
			return prefix.substring(0, prefix.length() - 1);
		}
		else {
			return prefix + BinderUtils.GROUP_INDEX_DELIMITER;
		}
	}

	private void checkNoConsumers(String queueName, Map<String, Object> queue) {
		if (!queue.get("consumers").equals(Integer.valueOf(0))) {
			throw new RabbitAdminException("Queue " + queueName + " is in use");
		}
	}

	@SuppressWarnings("unchecked")
	private List<String> findExchanges(String adminUri, String vhost, String binderPrefix, String entity,
			RestTemplate restTemplate, ExchangeCandidateCallback callback) {
		List<String> removedExchanges = new ArrayList<>();
		URI uri = UriComponentsBuilder.fromUriString(adminUri + "/api")
				.pathSegment("exchanges", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		List<Map<String, Object>> exchanges = restTemplate.getForObject(uri, List.class);
		for (Map<String, Object> exchange : exchanges) {
			String exchangeName = (String) exchange.get("name");
			if (callback.isCandidate(exchangeName)) {
				uri = UriComponentsBuilder.fromUriString(adminUri + "/api")
						.pathSegment("exchanges", "{vhost}", "{name}", "bindings", "source")
						.buildAndExpand(vhost, exchangeName).encode().toUri();
				List<Map<String, Object>> bindings = restTemplate.getForObject(uri, List.class);
				if (bindings.size() == 0) {
					uri = UriComponentsBuilder.fromUriString(adminUri + "/api")
							.pathSegment("exchanges", "{vhost}", "{name}", "bindings", "destination")
							.buildAndExpand(vhost, exchangeName).encode().toUri();
					bindings = restTemplate.getForObject(uri, List.class);
					if (bindings.size() == 0) {
						removedExchanges.add((String) exchange.get("name"));
					}
					else {
						throw new RabbitAdminException("Cannot delete exchange " + exchangeName
								+ "; it is a destination: " + bindings);
					}
				}
				else {
					throw new RabbitAdminException("Cannot delete exchange " + exchangeName + "; it has bindings: "
							+ bindings);
				}
			}
		}
		return removedExchanges;
	}

	private interface ExchangeCandidateCallback {

		boolean isCandidate(String exchangeName);
	}

}

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

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.BindingInfo;
import com.rabbitmq.http.client.domain.ExchangeInfo;
import com.rabbitmq.http.client.domain.QueueInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.BindingCleaner;

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
			Client client = new Client(adminUri, user, pw);
			return doClean(client,
					vhost == null ? "/" : vhost,
					binderPrefix == null ? BINDER_PREFIX : binderPrefix, entity, isJob);
		}
		catch (MalformedURLException | URISyntaxException e) {
			throw new RabbitAdminException("Couldn't create a Client", e);
		}
	}

	private Map<String, List<String>> doClean(Client client,
			String vhost, String binderPrefix, String entity, boolean isJob) {

		LinkedList<String> removedQueues = isJob ? null
				: findStreamQueues(client, vhost, binderPrefix, entity);
		List<String> removedExchanges = findExchanges(client, vhost, binderPrefix, entity);
		// Delete the queues in reverse order to enable re-running after a partial
		// success.
		// The queue search above starts with 0 and terminates on a not found.
		if (removedQueues != null) {
			removedQueues.descendingIterator().forEachRemaining(q -> {
				client.deleteQueue(vhost, q);
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
			client.deleteExchange(vhost, exchange);
			if (logger.isDebugEnabled()) {
				logger.debug("deleted exchange: " + exchange);
			}
		});
		if (removedExchanges.size() > 0) {
			results.put("exchanges", removedExchanges);
		}
		return results;
	}

	private LinkedList<String> findStreamQueues(Client client, String vhost, String binderPrefix, String stream) {
		String queueNamePrefix = adjustPrefix(AbstractBinder.applyPrefix(binderPrefix, stream));
		List<QueueInfo> queues = client.getQueues(vhost);
		return queues.stream()
			.filter(q -> q.getName().startsWith(queueNamePrefix))
			.map(q -> checkNoConsumers(q))
			.collect(Collectors.toCollection(LinkedList::new));
	}

	private String adjustPrefix(String prefix) {
		if (prefix.endsWith("*")) {
			return prefix.substring(0, prefix.length() - 1);
		}
		else {
			return prefix + PREFIX_DELIMITER;
		}
	}

	private String checkNoConsumers(QueueInfo queue) {
		if (queue.getConsumerCount() != 0) {
			throw new RabbitAdminException("Queue " + queue.getName() + " is in use");
		}
		return queue.getName();
	}

	private List<String> findExchanges(Client client, String vhost, String binderPrefix, String entity) {
		List<ExchangeInfo> exchanges = client.getExchanges(vhost);
		String exchangeNamePrefix = adjustPrefix(AbstractBinder.applyPrefix(binderPrefix, entity));
		List<String> exchangesToRemove = exchanges.stream()
				.filter(e -> e.getName().startsWith(exchangeNamePrefix))
				.map(e -> {
					System.out.println(e.getName());
					List<BindingInfo> bindingsBySource = client.getBindingsBySource(vhost, e.getName());
					return Collections.singletonMap(e.getName(), bindingsBySource);
				})
				.map(bindingsMap -> hasNoForeignBindings(bindingsMap, exchangeNamePrefix))
				.collect(Collectors.toList());
		exchangesToRemove.stream()
				.map(exchange -> client.getExchangeBindingsByDestination(vhost, exchange))
				.forEach(bindings -> {
					if (bindings.size() > 0) {
						throw new RabbitAdminException("Cannot delete exchange "
								+ bindings.get(0).getDestination() + "; it is a destination: " + bindings);
					}
				});
		return exchangesToRemove;
	}

	private String hasNoForeignBindings(Map<String, List<BindingInfo>> bindings, String exchangeNamePrefix) {
		Entry<String, List<BindingInfo>> next = bindings.entrySet().iterator().next();
		for (BindingInfo binding : next.getValue()) {
			if (!"queue".equals(binding.getDestinationType())
					|| !binding.getDestination().startsWith(exchangeNamePrefix)) {
				throw new RabbitAdminException("Cannot delete exchange "
						+ next.getKey() + "; it has bindings: " + bindings);
			}
		}
		return next.getKey();
	}

}

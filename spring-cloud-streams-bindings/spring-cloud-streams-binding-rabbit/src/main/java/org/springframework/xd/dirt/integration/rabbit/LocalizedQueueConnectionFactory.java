/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.rabbit;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.connection.RoutingConnectionFactory;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.xd.dirt.integration.bus.RabbitManagementUtils;


/**
 * A {@link RoutingConnectionFactory} that determines the node on which a queue is located and
 * returns a factory that connects directly to that node.
 * The RabbitMQ management plugin is called over REST to determine the node and the corresponding
 * address for that node is injected into the connection factory.
 * A single instance of each connection factory is retained in a cache.
 * If the location cannot be determined, the default connection factory is returned. This connection
 * factory is typically configured to connect to all the servers in a fail-over mode.
 * <p>{@link #getTargetConnectionFactory(Object)} is invoked by the
 * {@code SimpleMessageListenerContainer}, when establishing a connection, with the lookup key having
 * the format {@code '[queueName]'}.
 * <p>All {@link ConnectionFactory} methods delegate to the default
 *
 * @author Gary Russell
 * @since 1.2
 */
public class LocalizedQueueConnectionFactory implements ConnectionFactory, RoutingConnectionFactory {

	private final Log logger = LogFactory.getLog(getClass());

	private final Map<String, ConnectionFactory> nodeFactories = new HashMap<>();

	private final ConnectionFactory defaultConnectionFactory;

	private final String[] addresses;

	private final String[] adminAdresses;

	private final String[] nodes;

	private final String vhost;

	private final String username;

	private final String password;

	private final boolean useSSL;

	private final Resource sslPropertiesLocation;

	/**
	 *
	 * @param defaultConnectionFactory the fallback connection factory to use if the queue can't be located.
	 * @param addresses the rabbitmq server addresses (host:port, ...).
	 * @param adminAddresses the rabbitmq admin addresses (http://host:port, ...) must be the same length
	 * as addresses.
	 * @param nodes the rabbitmq nodes corresponding to addresses (rabbit@server1, ...).
	 * @param vhost the virtual host.
	 * @param username the user name.
	 * @param password the password.
	 */
	public LocalizedQueueConnectionFactory(ConnectionFactory defaultConnectionFactory,
			String[] addresses, String[] adminAddresses, String[] nodes, String vhost,
			String username, String password, boolean useSSL, Resource sslPropertiesLocation) {
		Assert.isTrue(addresses.length == adminAddresses.length
				&& addresses.length == nodes.length,
				"'addresses', 'adminAddresses', and 'nodes' properties must have equal length");
		this.defaultConnectionFactory = defaultConnectionFactory;
		this.addresses = Arrays.copyOf(addresses, addresses.length);
		this.adminAdresses = Arrays.copyOf(adminAddresses, adminAddresses.length);
		this.nodes = Arrays.copyOf(nodes, nodes.length);
		this.vhost = vhost;
		this.username = username;
		this.password = password;
		this.useSSL = useSSL;
		this.sslPropertiesLocation = sslPropertiesLocation;
	}

	@Override
	public Connection createConnection() throws AmqpException {
		return this.defaultConnectionFactory.createConnection();
	}

	@Override
	public String getHost() {
		return this.defaultConnectionFactory.getHost();
	}

	@Override
	public int getPort() {
		return this.defaultConnectionFactory.getPort();
	}

	@Override
	public String getVirtualHost() {
		return this.vhost;
	}

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		this.defaultConnectionFactory.addConnectionListener(listener);
	}

	@Override
	public boolean removeConnectionListener(ConnectionListener listener) {
		return this.defaultConnectionFactory.removeConnectionListener(listener);
	}

	@Override
	public void clearConnectionListeners() {
		this.defaultConnectionFactory.clearConnectionListeners();
	}

	@Override
	public ConnectionFactory getTargetConnectionFactory(Object key) {
		String queue = ((String) key);
		queue = queue.substring(1, queue.length() - 1);
		ConnectionFactory connectionFactory = determineConnectionFactory(queue);
		if (connectionFactory == null) {
			return this.defaultConnectionFactory;
		}
		else {
			return connectionFactory;
		}
	}

	private ConnectionFactory determineConnectionFactory(String queue) {
		for (int i = 0; i < this.adminAdresses.length; i++) {
			String adminUri = this.adminAdresses[i];
			RestTemplate template = createRestTemplate(adminUri);
			URI uri = UriComponentsBuilder.fromUriString(adminUri + "/api")
					.pathSegment("queues", "{vhost}", "{queue}")
					.buildAndExpand(this.vhost, queue).encode().toUri();
			try {
				@SuppressWarnings("unchecked")
				Map<String, Object> queueInfo = template.getForObject(uri, Map.class);
				if (queueInfo != null) {
					String node = (String) queueInfo.get("node");
					if (node != null) {
						for (int j = 0; j < this.nodes.length; j++) {
							if (this.nodes[j].equals(node)) {
								return nodeConnectionFactory(queue, j);
							}
						}
					}
				}
			}
			catch (Exception e) {
				logger.error("Failed to determine queue location for: " + queue + " at: " +
						uri.toString(), e);
			}
		}
		logger.warn("Failed to determine queue location for: " + queue);
		return null;
	}

	private synchronized ConnectionFactory nodeConnectionFactory(String queue, int index) throws Exception {
		String address = this.addresses[index];
		String node = this.nodes[index];
		if (logger.isDebugEnabled()) {
			logger.debug("Queue: " + queue + " is on node: " + node + " at: " + address);
		}
		ConnectionFactory cf = this.nodeFactories.get(node);
		if (cf == null) {
			if (logger.isDebugEnabled()) {
				logger.debug("Creating new connection factory for: " + address);
			}
			cf = createConnectionFactory(address);
			this.nodeFactories.put(node, cf);
		}
		return cf;
	}

	/**
	 * Create a RestTemplate for the supplied URI.
	 * @param adminUri the URI.
	 * @return the template.
	 */
	protected RestTemplate createRestTemplate(String adminUri) {
		return RabbitManagementUtils.buildRestTemplate(adminUri, this.username, this.password);
	}

	/**
	 * Create a dedicated connection factory for the address.
	 * @param address the address to which the factory should connect.
	 * @return the connection factory.
	 * @throws Exception if errors occur during creation.
	 */
	protected ConnectionFactory createConnectionFactory(String address) throws Exception {
		RabbitConnectionFactoryBean rcfb = new RabbitConnectionFactoryBean();
		rcfb.setUseSSL(this.useSSL);
		rcfb.setSslPropertiesLocation(this.sslPropertiesLocation);
		rcfb.afterPropertiesSet();
		CachingConnectionFactory ccf = new CachingConnectionFactory(rcfb.getObject());
		ccf.setAddresses(address);
		ccf.setUsername(this.username);
		ccf.setPassword(this.password);
		ccf.setVirtualHost(this.vhost);
		return ccf;
	}

}

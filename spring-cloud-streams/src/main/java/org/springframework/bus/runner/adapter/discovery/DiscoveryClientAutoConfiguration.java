/*
 * Copyright 2015 the original author or authors.
 *
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

package org.springframework.bus.runner.adapter.discovery;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.bus.runner.adapter.ChannelLocator;
import org.springframework.bus.runner.adapter.Output;
import org.springframework.bus.runner.adapter.MessageBusAdapter;
import org.springframework.bus.runner.adapter.Input;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.discovery.event.HeartbeatMonitor;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * Autoconfiguration for use when user has provided a
 * {@link DiscoveryClientChannelLocator}. Listens for changes in the service registry and
 * rebinds the external channels as needed.
 *
 * @author Dave Syer
 */
@Configuration
@ConditionalOnClass(DiscoveryClient.class)
public class DiscoveryClientAutoConfiguration {

	private HeartbeatMonitor monitor = new HeartbeatMonitor();

	@Autowired
	private MessageBusAdapter adapter;

	@Autowired(required = false)
	@Input
	private ChannelLocator inputChannelLocator;

	@Autowired(required = false)
	@Output
	private ChannelLocator outputChannelLocator;

	private boolean enabled = false;

	@PostConstruct
	public void init() {
		if (this.inputChannelLocator instanceof DiscoveryClientChannelLocator
				|| this.outputChannelLocator instanceof DiscoveryClientChannelLocator) {
			this.enabled = true;
		}
	}

	@EventListener
	public void discoveryHeartbeat(HeartbeatEvent event) {
		if (this.enabled && this.monitor.update(event.getValue())) {
			this.adapter.rebind();
		}
	}

}

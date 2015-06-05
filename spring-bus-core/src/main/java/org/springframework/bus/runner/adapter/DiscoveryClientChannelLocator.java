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

package org.springframework.bus.runner.adapter;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * @author Dave Syer
 *
 */
public class DiscoveryClientChannelLocator implements ChannelLocator {

	private Log logger = LogFactory.getLog(DiscoveryClientChannelLocator.class);

	private DiscoveryClient discovery;

	private RestOperations restTemplate = new RestTemplate();

	private String serviceId;

	public DiscoveryClientChannelLocator(DiscoveryClient discovery, String serviceId) {
		this.discovery = discovery;
		this.serviceId = serviceId;
	}

	public void setRestTemplate(RestOperations restTemplate) {
		this.restTemplate = restTemplate;
	}

	@Override
	public String locate(String name) {
		List<ServiceInstance> instances = this.discovery.getInstances(this.serviceId);
		if (instances == null || instances.isEmpty()) {
			return null;
		}
		URI uri = pickUrl(instances);
		try {
			ChannelsMetadata channels = this.restTemplate.getForObject(uri,
					ChannelsMetadata.class);
			Collection<? extends InputChannelSpec> specs = Collections.emptySet();
			if (name.startsWith("input")) {
				name = name.replace("input", "output");
				specs = channels.getOutputChannels();
			}
			else if (name.startsWith("output")) {
				name = name.replace("output", "input");
				specs = channels.getInputChannels();
			}
			for (InputChannelSpec spec : specs) {
				if (name.equals(spec.getLocalName())) {
					this.logger.debug("Discovered channel for '" + this.serviceId + "' ("
							+ name + "=" + spec.getName() + ")");
					return spec.getName();
				}
			}
		}
		catch (Exception e) {
			this.logger.warn("Could not discover channel for '" + this.serviceId + "' ("
					+ e.getClass() + ": " + e.getMessage() + ")");
			return null;
		}
		this.logger.warn("No channel disccovered for '" + this.serviceId + "' (" + name + ")");
		return null;
	}

	private URI pickUrl(List<ServiceInstance> instances) {
		return UriComponentsBuilder
				.fromUri(instances.get(new Random().nextInt(instances.size())).getUri())
				.path("channels").build().toUri();
	}

}

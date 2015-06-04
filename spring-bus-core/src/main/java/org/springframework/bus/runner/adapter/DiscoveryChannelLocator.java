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
import java.util.List;
import java.util.Random;

import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.client.RestTemplate;

/**
 * @author Dave Syer
 *
 */
public class DiscoveryChannelLocator implements ChannelLocator {

	private DiscoveryClient discovery;
	
	private RestTemplate restTemplate = new RestTemplate();

	private String serviceId;

	public DiscoveryChannelLocator(DiscoveryClient discovery, String serviceId) {
		this.discovery = discovery;
		this.serviceId = serviceId;
	}

	@Override
	public String locate(String name) {
		List<ServiceInstance> instances = discovery.getInstances(serviceId);
		if (instances==null || instances.isEmpty()) {
			return null;
		}
		URI uri = pickUrl(instances);
		try {
			ChannelsMetadata channels = restTemplate.getForObject(uri, ChannelsMetadata.class);
			for (OutputChannelSpec spec : channels.getOutputChannels()) {
				if (name.equals(spec.getLocalName())) {
					return spec.getName();
				}
			}
			for (InputChannelSpec spec : channels.getInputChannels()) {
				if (name.equals(spec.getLocalName())) {
					return spec.getName();
				}
			}
		} catch (Exception e) {
			return null;
		}
		return null;
	}

	private URI pickUrl(List<ServiceInstance> instances) {
		return instances.get(new Random().nextInt(instances.size())).getUri();
	}

}

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

package org.springframework.cloud.stream.binder.local.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.local.LocalMessageChannelBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.scheduling.support.PeriodicTrigger;

/**
 * @author David Turanski
 */
@Configuration
@ConfigurationProperties(prefix = "spring.cloud.stream.binder.local")
@EnableConfigurationProperties(LocalExecutorConfigurationProperties.class)
public class LocalMessageChannelBinderConfiguration {
	
	private int queueSize = Integer.MAX_VALUE;
	
	private int polling;
	
	@Autowired
	LocalExecutorConfigurationProperties localExecutorConfigurationProperties;

	@Bean
	public LocalMessageChannelBinder localMessageChannelBinder() {
		LocalMessageChannelBinder localMessageChannelBinder = new LocalMessageChannelBinder();
		
		localMessageChannelBinder.setExecutorCorePoolSize(localExecutorConfigurationProperties.getCorePoolSize());
		localMessageChannelBinder.setExecutorKeepAliveSeconds(localExecutorConfigurationProperties.getKeepAliveSeconds());
		localMessageChannelBinder.setExecutorMaxPoolSize(localExecutorConfigurationProperties.getMaxPoolSize());
		localMessageChannelBinder.setExecutorQueueSize(localExecutorConfigurationProperties.getQueueSize());

		if (polling > 0) {
			PollerMetadata pollerMetadata = new PollerMetadata();
			pollerMetadata.setTrigger(new PeriodicTrigger(polling));
			localMessageChannelBinder.setPoller(pollerMetadata);
		}
		
		localMessageChannelBinder.setQueueSize(queueSize);

		return localMessageChannelBinder;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public void setPolling(int polling) {
		this.polling = polling;
	}
	
}

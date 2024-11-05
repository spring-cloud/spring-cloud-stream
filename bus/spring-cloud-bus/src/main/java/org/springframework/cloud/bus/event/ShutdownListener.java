/*
 * Copyright 2012-2024 the original author or authors.
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

package org.springframework.cloud.bus.event;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;

/**
 * @author Ryan Baxter
 */
public class ShutdownListener implements ApplicationListener<ShutdownRemoteApplicationEvent>, ApplicationContextAware {

	private static final Log LOG = LogFactory.getLog(ShutdownListener.class);

	private ApplicationContext context;

	private ServiceMatcher serviceMatcher;

	public ShutdownListener(ServiceMatcher serviceMatcher) {
		this.serviceMatcher = serviceMatcher;
	}

	@Override
	public void onApplicationEvent(ShutdownRemoteApplicationEvent event) {
		if (serviceMatcher.isForSelf(event)) {
			LOG.warn("Received remote shutdown request from " + event.getOriginService() + ".  Shutting down.");
			shutdown();
		}
		else {
			LOG.info("Shutdown not performed, the event was targeting " + event.getDestinationService());
		}

	}

	protected int shutdown() {
		return SpringApplication.exit(context, () -> 0);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}

}

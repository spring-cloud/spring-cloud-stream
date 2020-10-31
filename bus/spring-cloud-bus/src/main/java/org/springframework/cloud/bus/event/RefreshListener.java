/*
 * Copyright 2012-2019 the original author or authors.
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

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.context.ApplicationListener;

/**
 * @author Spencer Gibb
 * @author Ryan Baxter
 */
public class RefreshListener implements ApplicationListener<RefreshRemoteApplicationEvent> {

	private static Log log = LogFactory.getLog(RefreshListener.class);

	private ContextRefresher contextRefresher;

	private ServiceMatcher serviceMatcher;

	public RefreshListener(ContextRefresher contextRefresher, ServiceMatcher serviceMatcher) {
		this.contextRefresher = contextRefresher;
		this.serviceMatcher = serviceMatcher;
	}

	@Override
	public void onApplicationEvent(RefreshRemoteApplicationEvent event) {
		log.info("Received remote refresh request.");
		if (serviceMatcher.isForSelf(event)) {
			Set<String> keys = this.contextRefresher.refresh();
			log.info("Keys refreshed " + keys);
		}
		else {
			log.info("Refresh not performed, the event was targeting " + event.getDestinationService());
		}
	}

}

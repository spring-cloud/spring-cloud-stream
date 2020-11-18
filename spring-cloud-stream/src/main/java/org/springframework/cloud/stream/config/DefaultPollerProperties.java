/*
 * Copyright 2015-2020 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;

/**
 * @author Dave Syer
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 */
@ConfigurationProperties("spring.cloud.stream.poller")
public class DefaultPollerProperties {

	/**
	 * Fixed delay for default poller.
	 */
	private long fixedDelay = 1000L;

	/**
	 * Maximum messages per poll for the default poller.
	 */
	private long maxMessagesPerPoll = 1L;

	/**
	 * Cron expression value for the Cron Trigger.
	 */
	private String cron;

	/**
	 * Initial delay for periodic triggers.
	 */
	private int initialDelay = 0;

	/**
	 * The TimeUnit to apply to delay values.
	 */
	private TimeUnit timeUnit = TimeUnit.MILLISECONDS;

	public PollerMetadata getPollerMetadata() {
		PollerMetadata pollerMetadata = new PollerMetadata();
		if (cron != null) {
			pollerMetadata.setTrigger(new CronTrigger(cron));
		}
		else {
			final PeriodicTrigger periodicTrigger = new PeriodicTrigger(this.fixedDelay, this.timeUnit);
			periodicTrigger.setInitialDelay(initialDelay);
			pollerMetadata.setTrigger(periodicTrigger);
		}

		pollerMetadata.setMaxMessagesPerPoll(this.maxMessagesPerPoll);
		return pollerMetadata;
	}

	public long getFixedDelay() {
		return this.fixedDelay;
	}

	public void setFixedDelay(long fixedDelay) {
		this.fixedDelay = fixedDelay;
	}

	public long getMaxMessagesPerPoll() {
		return this.maxMessagesPerPoll;
	}

	public void setMaxMessagesPerPoll(long maxMessagesPerPoll) {
		this.maxMessagesPerPoll = maxMessagesPerPoll;
	}

	public String getCron() {
		return cron;
	}

	public void setCron(String cron) {
		this.cron = cron;
	}

	public int getInitialDelay() {
		return initialDelay;
	}

	public void setInitialDelay(int initialDelay) {
		this.initialDelay = initialDelay;
	}

	public TimeUnit getTimeUnit() {
		return timeUnit;
	}

	public void setTimeUnit(TimeUnit timeUnit) {
		this.timeUnit = timeUnit;
	}
}

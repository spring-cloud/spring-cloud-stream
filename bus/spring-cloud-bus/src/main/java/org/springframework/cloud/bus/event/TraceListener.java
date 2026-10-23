/*
 * Copyright 2012-present the original author or authors.
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

import java.time.Clock;
import java.time.Instant;

import org.springframework.cloud.bus.trace.Trace;
import org.springframework.cloud.bus.trace.TraceRepository;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

/**
 * A listener for application event sends and acks. Inserts a record for each signal into
 * the {@link TraceRepository}.
 *
 * @author Dave Syer
 * @author Ngoc Nhan
 */
public class TraceListener implements ApplicationListener<ApplicationEvent> {

	private final TraceRepository repository;

	public TraceListener(TraceRepository repository) {
		this.repository = repository;
	}

	@Override
	public void onApplicationEvent(ApplicationEvent event) {

		if (event instanceof AckRemoteApplicationEvent ackRemoteApplicationEvent) {
			this.repository.add(getReceivedTrace(ackRemoteApplicationEvent));
		}

		if (event instanceof SentApplicationEvent sentApplicationEvent) {
			this.repository.add(getSentTrace(sentApplicationEvent));
		}

	}

	/**
	 * Creates a trace for a acks application event.
	 * @param event the acks application event
	 * @return the trace for the acks application event
	 */
	protected Trace getReceivedTrace(AckRemoteApplicationEvent event) {

		Trace trace = new Trace(Instant.now(Clock.systemUTC()), "spring.cloud.bus.ack",
				event.getEvent().getSimpleName());
		trace.setId(event.getAckId());
		trace.setOrigin(event.getOriginService());
		trace.setDestination(event.getAckDestinationService());
		return trace;
	}

	/**
	 * Creates a trace for a sent application event.
	 * @param event the sent application event
	 * @return the trace for the sent application event
	 */
	protected Trace getSentTrace(SentApplicationEvent event) {

		Trace trace = new Trace(Instant.now(Clock.systemUTC()), "spring.cloud.bus.sent",
				event.getType().getSimpleName());
		trace.setId(event.getId());
		trace.setOrigin(event.getOriginService());
		trace.setDestination(event.getDestinationService());
		return trace;
	}

}

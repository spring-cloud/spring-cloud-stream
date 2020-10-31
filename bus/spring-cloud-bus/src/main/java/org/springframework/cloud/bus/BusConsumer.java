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

package org.springframework.cloud.bus;

import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.bus.event.AckRemoteApplicationEvent;
import org.springframework.cloud.bus.event.Destination;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.bus.event.SentApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

public class BusConsumer implements Consumer<RemoteApplicationEvent> {

	private final Log log = LogFactory.getLog(getClass());

	private final ApplicationEventPublisher publisher;

	private final ServiceMatcher serviceMatcher;

	private final ObjectProvider<BusBridge> busBridge;

	private final BusProperties properties;

	private final Destination.Factory destinationFactory;

	public BusConsumer(ApplicationEventPublisher publisher, ServiceMatcher serviceMatcher,
			ObjectProvider<BusBridge> busBridge, BusProperties properties, Destination.Factory destinationFactory) {
		this.publisher = publisher;
		this.serviceMatcher = serviceMatcher;
		this.busBridge = busBridge;
		this.properties = properties;
		this.destinationFactory = destinationFactory;
	}

	@Override
	public void accept(RemoteApplicationEvent event) {
		if (event instanceof AckRemoteApplicationEvent) {
			if (this.properties.getTrace().isEnabled() && !this.serviceMatcher.isFromSelf(event)
					&& this.publisher != null) {
				this.publisher.publishEvent(event);
			}
			// If it's an ACK we are finished processing at this point
			return;
		}

		if (log.isDebugEnabled()) {
			log.debug("Received remote event from bus: " + event);
		}

		if (this.serviceMatcher.isForSelf(event) && this.publisher != null) {
			if (!this.serviceMatcher.isFromSelf(event)) {
				this.publisher.publishEvent(event);
			}
			if (this.properties.getAck().isEnabled()) {
				AckRemoteApplicationEvent ack = new AckRemoteApplicationEvent(this, this.serviceMatcher.getBusId(),
						destinationFactory.getDestination(this.properties.getAck().getDestinationService()),
						event.getDestinationService(), event.getId(), event.getClass());
				this.busBridge.ifAvailable(bridge -> bridge.send(ack));
				this.publisher.publishEvent(ack);
			}
		}
		if (this.properties.getTrace().isEnabled() && this.publisher != null) {
			// We are set to register sent events so publish it for local consumption,
			// irrespective of the origin
			this.publisher.publishEvent(new SentApplicationEvent(this, event.getOriginService(),
					event.getDestinationService(), event.getId(), event.getClass()));
		}
	}

}

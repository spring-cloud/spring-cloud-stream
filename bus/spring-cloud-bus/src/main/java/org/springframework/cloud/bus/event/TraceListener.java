/*
 * Copyright 2013-2017 the original author or authors.
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
 *
 */

package org.springframework.cloud.bus.event;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.trace.http.HttpTraceRepository;
import org.springframework.context.event.EventListener;

/**
 * A listener for sends and acks of remote application events. Inserts a record for each
 * signal in the {@link HttpTraceRepository}.
 *
 * @author Dave Syer
 */
public class TraceListener {

	private static Log log = LogFactory.getLog(TraceListener.class);

	private HttpTraceRepository repository;

	public TraceListener(HttpTraceRepository repository) {
		this.repository = repository;
	}

	@EventListener
	public void onAck(AckRemoteApplicationEvent event) {
		Map<String, Object> trace = getReceivedTrace(event);
		// FIXME boot 2 this.repository.add(trace);
	}

	@EventListener
	public void onSend(SentApplicationEvent event) {
		Map<String, Object> trace = getSentTrace(event);
		// FIXME boot 2 this.repository.add(trace);
	}

	protected Map<String, Object> getSentTrace(SentApplicationEvent event) {
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		map.put("signal", "spring.cloud.bus.sent");
		map.put("type", event.getType().getSimpleName());
		map.put("id", event.getId());
		map.put("origin", event.getOriginService());
		map.put("destination", event.getDestinationService());
		if (log.isDebugEnabled()) {
			log.debug(map);
		}
		return map;
	}

	protected Map<String, Object> getReceivedTrace(AckRemoteApplicationEvent event) {
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		map.put("signal", "spring.cloud.bus.ack");
		map.put("event", event.getEvent().getSimpleName());
		map.put("id", event.getAckId());
		map.put("origin", event.getOriginService());
		map.put("destination", event.getAckDestinationService());
		if (log.isDebugEnabled()) {
			log.debug(map);
		}
		return map;
	}
}

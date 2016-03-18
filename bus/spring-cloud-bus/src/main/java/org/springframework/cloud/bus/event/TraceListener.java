package org.springframework.cloud.bus.event;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.trace.TraceRepository;
import org.springframework.context.event.EventListener;

/**
 * A listener for sends and acks of remote application events. Inserts a record for each
 * signal in the {@link TraceRepository}.
 *
 * @author Dave Syer
 */
public class TraceListener {

	private static Log log = LogFactory.getLog(TraceListener.class);

	private TraceRepository repository;

	public TraceListener(TraceRepository repository) {
		this.repository = repository;
	}

	@EventListener
	public void onAck(AckRemoteApplicationEvent event) {
		this.repository.add(getReceivedTrace(event));
	}

	@EventListener
	public void onSend(SentApplicationEvent event) {
		this.repository.add(getSentTrace(event));
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

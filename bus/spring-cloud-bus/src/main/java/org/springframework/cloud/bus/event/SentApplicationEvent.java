package org.springframework.cloud.bus.event;

import org.springframework.context.ApplicationEvent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * An event signalling that a remote event was sent somewhere in the system. This is not
 * itself a {@link RemoteApplicationEvent}, so it isn't sent over the bus, instead it is
 * generated locally (possibly in response to a remote event). Applications that want to
 * audit remote events can listen for this one and the {@link AckRemoteApplicationEvent}
 * from all the consumers (the {@link #getId() id} of this event is the
 * {@link AckRemoteApplicationEvent#getAckId() ackId} of the corresponding ACK.
 *
 * @author Dave Syer
 */
@SuppressWarnings("serial")
@Data
@EqualsAndHashCode(callSuper = false)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonIgnoreProperties("source")
public class SentApplicationEvent extends ApplicationEvent {

	private static final Object TRANSIENT_SOURCE = new Object();
	private final String originService;
	private final String destinationService;
	private final String id;
	private Class<? extends RemoteApplicationEvent> type;

	protected SentApplicationEvent() {
		// for serialization libs like jackson
		this(TRANSIENT_SOURCE, null, null, null, RemoteApplicationEvent.class);
	}

	public SentApplicationEvent(Object source, String originService,
			String destinationService, String id,
			Class<? extends RemoteApplicationEvent> type) {
		super(source);
		this.originService = originService;
		this.type = type;
		if (destinationService == null) {
			destinationService = "*";
		}
		if (!destinationService.contains(":")) {
			// All instances of the destination unless specifically requested
			destinationService = destinationService + ":**";
		}
		this.destinationService = destinationService;
		this.id = id;
	}
}

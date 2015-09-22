package org.springframework.cloud.bus.event;

import org.springframework.context.ApplicationEvent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author Spencer Gibb
 */
@SuppressWarnings("serial")
@Data
@EqualsAndHashCode(callSuper = false)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonIgnoreProperties("source")
public abstract class RemoteApplicationEvent extends ApplicationEvent {
	private static final Object TRANSIENT_SOURCE = new Object();
	private final String originService;
	private final String destinationService;

	protected RemoteApplicationEvent() {
		// for serialization libs like jackson
		super(TRANSIENT_SOURCE);
		this.originService = null;
		this.destinationService = null;
	}

	protected RemoteApplicationEvent(Object source, String originService,
			String destinationService) {
		super(source);
		this.originService = originService;
		if (destinationService == null) {
			destinationService = "*";
		}
		if (!destinationService.contains(":")) {
			// All instances of the destination unless specifically requested
			destinationService = destinationService + ":**";
		}
		this.destinationService = destinationService;
	}

	protected RemoteApplicationEvent(Object source, String originService) {
		this(source, originService, null);
	}
}

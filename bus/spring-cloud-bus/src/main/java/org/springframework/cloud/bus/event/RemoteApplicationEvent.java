package org.springframework.cloud.bus.event;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.springframework.context.ApplicationEvent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

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
		originService = null;
		destinationService = null;
	}

	protected RemoteApplicationEvent(Object source, String originService,
			String destinationService) {
		super(source);
		this.originService = originService;
		this.destinationService = destinationService;
	}

	protected RemoteApplicationEvent(Object source, String originService) {
		this(source, originService, null);
	}
}

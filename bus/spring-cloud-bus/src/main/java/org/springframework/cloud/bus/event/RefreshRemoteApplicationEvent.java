package org.springframework.cloud.bus.event;

/**
 * @author Spencer Gibb
 */
@SuppressWarnings("serial")
public class RefreshRemoteApplicationEvent extends RemoteApplicationEvent {

	@SuppressWarnings("unused")
	private RefreshRemoteApplicationEvent() {
		// for serializers
	}

	public RefreshRemoteApplicationEvent(Object source, String originService,
			String destinationService) {
		super(source, originService, destinationService);
	}
}

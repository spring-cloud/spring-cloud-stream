package org.springframework.cloud.bus.event;

/**
 * @author Spencer Gibb
 */
@SuppressWarnings("serial")
public class RefreshRemoteApplicationEvent extends RemoteApplicationEvent {
    public RefreshRemoteApplicationEvent(Object source, String originService, String destinationService) {
        super(source, originService, destinationService);
    }
}

package org.springframework.platform.bus.event;

/**
 * @author Spencer Gibb
 */
public class RefreshRemoteApplicationEvent extends RemoteApplicationEvent {
    public RefreshRemoteApplicationEvent(Object source, String originService) {
        super(source, originService);
    }
}

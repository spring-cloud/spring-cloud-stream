package org.springframework.cloud.bus.event;

/**
 * @author Spencer Gibb
 */
@SuppressWarnings("serial,unused")
public class RefreshRemoteApplicationEvent extends RemoteApplicationEvent {

    private RefreshRemoteApplicationEvent(){
        //for serializers
    }

    public RefreshRemoteApplicationEvent(Object source, String originService, String destinationService) {
        super(source, originService, destinationService);
    }
}

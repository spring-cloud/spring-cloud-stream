package org.springframework.cloud.bus.event;

import lombok.Data;
import org.springframework.context.ApplicationEvent;

/**
 * @author Spencer Gibb
 */
@Data
public class RemoteApplicationEvent extends ApplicationEvent {
    private final String originService;
    private final String destinationService;

    public RemoteApplicationEvent(Object source, String originService, String destinationService) {
        super(source);
        this.originService = originService;
        this.destinationService = destinationService;
    }

    public RemoteApplicationEvent(Object source, String originService) {
        this(source, originService, null);
    }
}

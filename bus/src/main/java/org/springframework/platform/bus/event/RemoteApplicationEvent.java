package org.springframework.platform.bus.event;

import lombok.Data;
import org.springframework.context.ApplicationEvent;

/**
 * @author Spencer Gibb
 */
@Data
public class RemoteApplicationEvent extends ApplicationEvent {
    private final String originService;

    public RemoteApplicationEvent(Object source, String originService) {
        super(source);
        this.originService = originService;
    }
}

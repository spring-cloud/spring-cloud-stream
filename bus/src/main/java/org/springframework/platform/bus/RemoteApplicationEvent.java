package org.springframework.platform.bus;

import lombok.Data;
import org.springframework.context.ApplicationEvent;

/**
 * @author Spencer Gibb
 */
@Data
public class RemoteApplicationEvent extends ApplicationEvent {
    private final String originService;
    private final String message;

    public RemoteApplicationEvent(Object source, String originService, String message) {
        super(source);
        this.originService = originService;
        this.message = message;
    }
}

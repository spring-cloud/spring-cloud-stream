package org.springframework.cloud.bus.hystrix;

import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;

/**
 * @author Spencer Gibb
 */
@MessagingGateway
public interface HystrixStreamChannel {

    @Gateway(requestChannel = "hystrixStream")
    public void send(String s);
}

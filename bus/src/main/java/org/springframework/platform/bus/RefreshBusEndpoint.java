package org.springframework.platform.bus;

import org.springframework.platform.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author Spencer Gibb
 */
public class RefreshBusEndpoint extends AbstractBusEndpoint {

    @RequestMapping(value = "refresh", method = RequestMethod.POST)
    @ResponseBody
    public void refresh(@RequestBody String destination) {
        context.publishEvent(new RefreshRemoteApplicationEvent(this, getAppName(), destination));
    }


}

package org.springframework.platform.bus.endpoint;

import org.springframework.platform.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author Spencer Gibb
 */
public class RefreshBusEndpoint extends AbstractBusEndpoint {

    @RequestMapping(value = "refresh", method = RequestMethod.POST)
    @ResponseBody
    public void refresh(@RequestParam(value = "destination", required = false) String destination) {
        publish(new RefreshRemoteApplicationEvent(this, getAppName(), destination));
    }


}

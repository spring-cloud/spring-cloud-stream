package org.springframework.platform.bus;

import org.springframework.platform.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

/**
 * @author Spencer Gibb
 */
public class RefreshBusEndpoint extends AbstractBusEndpoint {

    @RequestMapping(value = "refresh", method = RequestMethod.POST)
    @ResponseBody
    public void refresh(@RequestParam Map<String, String> params) {
        context.publishEvent(new RefreshRemoteApplicationEvent(this, getAppName()));
    }


}

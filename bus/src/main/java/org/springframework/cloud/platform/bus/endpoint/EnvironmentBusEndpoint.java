package org.springframework.cloud.bus.endpoint;

import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

/**
 * @author Spencer Gibb
 */
public class EnvironmentBusEndpoint extends AbstractBusEndpoint {

    @RequestMapping(value = "env", method = RequestMethod.POST)
    @ResponseBody
    //TODO: make this an abstract method in AbstractBusEndpoint?
    public void env(@RequestParam Map<String, String> params,
                        @RequestParam(value = "destination", required = false) String destination) {
        publish(new EnvironmentChangeRemoteApplicationEvent(this, getAppName(), destination, params));
    }


}

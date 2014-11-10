package org.springframework.cloud.bus.endpoint;

import static org.junit.Assert.assertEquals;

import org.junit.Test;



/**
 * @author Dave Syer
 */
public class RefreshBusEndpointTests {
		
	@Test
	public void instanceId() throws Exception {
		RefreshBusEndpoint endpoint = new RefreshBusEndpoint(null, "foo", new BusEndpoint());
		assertEquals("foo", endpoint.getInstanceId());
	}

}

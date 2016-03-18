package org.springframework.cloud.bus.jackson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Spencer Gibb
 */
public class SubtypeModuleTests {

	@Test
	public void testSubclass() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new SubtypeModule(MyRemoteApplicationEvent.class));

		RemoteApplicationEvent event = mapper.readValue(
				"{\"type\":\"my\", \"destinationService\":\"myservice\", \"originService\":\"myorigin\"}",
				RemoteApplicationEvent.class);
		assertTrue("event is wrong type", event instanceof MyRemoteApplicationEvent);
		MyRemoteApplicationEvent myEvent = MyRemoteApplicationEvent.class.cast(event);
		assertEquals("originService was wrong", "myorigin", myEvent.getOriginService());
		assertEquals("destinationService was wrong", "myservice",
				myEvent.getDestinationService());

		event = mapper.readValue("{\"type\":\"another\"}",
				AnotherRemoteApplicationEvent.class);
		assertTrue("event is wrong type", event instanceof AnotherRemoteApplicationEvent);
	}

	@SuppressWarnings("serial")
	@JsonTypeName("my")
	public static class MyRemoteApplicationEvent extends RemoteApplicationEvent {
		@SuppressWarnings("unused")
		private MyRemoteApplicationEvent() {
		}

		protected MyRemoteApplicationEvent(Object source, String originService,
				String destinationService) {
			super(source, originService, destinationService);
		}

		protected MyRemoteApplicationEvent(Object source, String originService) {
			super(source, originService);
		}
	}

	@SuppressWarnings("serial")
	@JsonTypeName("another")
	public static class AnotherRemoteApplicationEvent extends RemoteApplicationEvent {
		@SuppressWarnings("unused")
		private AnotherRemoteApplicationEvent() {
		}

		protected AnotherRemoteApplicationEvent(Object source, String originService,
				String destinationService) {
			super(source, originService, destinationService);
		}

		protected AnotherRemoteApplicationEvent(Object source, String originService) {
			super(source, originService);
		}
	}
}

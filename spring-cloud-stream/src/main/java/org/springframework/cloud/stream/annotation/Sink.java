package org.springframework.cloud.stream.annotation;

import org.springframework.messaging.SubscribableChannel;

public interface Sink {

	public static String INPUT = "input";

	@Input(Sink.INPUT)
	SubscribableChannel input();

}

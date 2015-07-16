package org.springframework.cloud.stream.annotation;

import org.springframework.messaging.MessageChannel;

public interface Source {
	
	public static String OUTPUT = "output";
	
	@Output(Source.OUTPUT)
	MessageChannel output();

}

	
	
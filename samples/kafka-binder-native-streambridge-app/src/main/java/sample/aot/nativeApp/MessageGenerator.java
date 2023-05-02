package sample.aot.nativeApp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component

public class MessageGenerator {
	@Autowired
	private StreamBridge streamBridge;

	private boolean running = false;

	public void stop() {running = false;};

	@Async
	public void start() {
		try{
			running = true;
			while(running) {
				String partitionKeyValue = UUID.randomUUID().toString();
				streamBridge.send("sampleTopic", MessageBuilder.withPayload("sampleContent")
					.setHeader("partitionKey", partitionKeyValue)
					.build());
				Thread.sleep(1000);
			}
		} catch (Exception e) {}
	}

}

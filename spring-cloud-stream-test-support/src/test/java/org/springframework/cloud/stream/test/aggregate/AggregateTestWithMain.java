package org.springframework.cloud.stream.test.aggregate;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.springframework.cloud.stream.aggregate.AggregateApplication;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 */
public class AggregateTestWithMain {

	@Test
	public void testAggregateApplication() throws InterruptedException {
		// emulate a main method
		ConfigurableApplicationContext context = new AggregateApplicationBuilder().from(UppercaseProcessor.class)
				.namespace("upper").to(SuffixProcessor.class).namespace("suffix").run();

		AggregateApplication aggregateAccessor = context.getBean(AggregateApplication.class);
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Processor uppercaseProcessor = aggregateAccessor.getBinding(Processor.class, "upper");
		Processor suffixProcessor = aggregateAccessor.getBinding(Processor.class, "suffix");
		uppercaseProcessor.input().send(MessageBuilder.withPayload("Hello").build());
		Message<?> receivedMessage = messageCollector.forChannel(suffixProcessor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo("HELLO WORLD!");
		context.close();
	}

	@Configuration
	@EnableBinding(Processor.class)
	public static class UppercaseProcessor {

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in.toUpperCase();
		}
	}

	@Configuration
	@EnableBinding(Processor.class)
	public static class SuffixProcessor {

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in + " WORLD!";
		}
	}
}

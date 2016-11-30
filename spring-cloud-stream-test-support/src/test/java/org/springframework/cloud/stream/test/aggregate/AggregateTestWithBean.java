package org.springframework.cloud.stream.test.aggregate;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.aggregate.AggregateApplication;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AggregateTestWithBean.ChainedProcessors.class, properties = {"server.port=-1"})
public class AggregateTestWithBean {

	@Autowired
	public MessageCollector messageCollector;

	@Autowired
	public AggregateApplication aggregateApplication;

	@Test
	public void testAggregateApplication() throws InterruptedException {
		Processor uppercaseProcessor = aggregateApplication.getBinding(Processor.class, "upper");
		Processor suffixProcessor = aggregateApplication.getBinding(Processor.class, "suffix");
		uppercaseProcessor.input().send(MessageBuilder.withPayload("Hello").build());
		Message<?> receivedMessage = messageCollector.forChannel(suffixProcessor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo("HELLO WORLD!");
	}

	@SpringBootApplication
	@EnableBinding
	public static class ChainedProcessors {

		@Bean
		public AggregateApplication aggregateApplication() {
			return new AggregateApplicationBuilder().from(UppercaseProcessor.class)
					.namespace("upper").to(SuffixProcessor.class).namespace("suffix").build();
		}
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

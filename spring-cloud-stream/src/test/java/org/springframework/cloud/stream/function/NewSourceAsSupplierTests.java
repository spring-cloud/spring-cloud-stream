package org.springframework.cloud.stream.function;

import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

public class NewSourceAsSupplierTests {

	@Test
	public void testSourceFromSupplier() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SourceFromSupplier.class)).web(
			WebApplicationType.NONE).run("--spring.cloud.stream.function.name=date", "--spring.jmx.enabled=false")) {

			OutputDestination target = context.getBean(OutputDestination.class);
			Message<byte[]> sourceMessage = target.receive(10000);
			System.out.println(sourceMessage);
//			assertThat(target.receive(10000).getPayload()).isEqualTo("1".getBytes(StandardCharsets.UTF_8));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("2".getBytes(StandardCharsets.UTF_8));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("3".getBytes(StandardCharsets.UTF_8));
			//etc
		}
	}

	@Test
	public void testProcessorFromFunction() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ProcessorFromFunction.class)).web(
			WebApplicationType.NONE).run("--spring.cloud.stream.function.name=toUpperCase", "--spring.jmx.enabled=false")) {

			InputDestination source = context.getBean(InputDestination.class);
			source.send(new GenericMessage<byte[]>("fopo".getBytes()));
			OutputDestination target = context.getBean(OutputDestination.class);
			Message<byte[]> targetMessage = target.receive(10000);
			System.out.println(new String(targetMessage.getPayload()));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("1".getBytes(StandardCharsets.UTF_8));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("2".getBytes(StandardCharsets.UTF_8));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("3".getBytes(StandardCharsets.UTF_8));
			//etc
		}
	}

	@Test
	public void testSinkFromConsumer() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SinkFromConsumer.class)).web(
			WebApplicationType.NONE).run("--spring.cloud.stream.function.name=sink", "--spring.jmx.enabled=false")) {

			InputDestination source = context.getBean(InputDestination.class);
			source.send(new GenericMessage<byte[]>("fopo".getBytes()));
//			OutputDestination target = context.getBean(OutputDestination.class);
//			Message<byte[]> targetMessage = target.receive(10000);
//			System.out.println(new String(targetMessage.getPayload()));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("1".getBytes(StandardCharsets.UTF_8));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("2".getBytes(StandardCharsets.UTF_8));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("3".getBytes(StandardCharsets.UTF_8));
			//etc
		}
	}

	@Test
	public void testSinkFromConsumerNoEnableBinding() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SinkFromConsumerNoEnableBinding.class)).web(
			WebApplicationType.NONE).run("--spring.cloud.stream.function.name=sink", "--spring.jmx.enabled=false")) {

			InputDestination source = context.getBean(InputDestination.class);
			source.send(new GenericMessage<byte[]>("Hello No Binding".getBytes()));
//			OutputDestination target = context.getBean(OutputDestination.class);
//			Message<byte[]> targetMessage = target.receive(10000);
//			System.out.println(new String(targetMessage.getPayload()));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("1".getBytes(StandardCharsets.UTF_8));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("2".getBytes(StandardCharsets.UTF_8));
//			assertThat(target.receive(10000).getPayload()).isEqualTo("3".getBytes(StandardCharsets.UTF_8));
			//etc
		}
	}


	@EnableAutoConfiguration
	@EnableBinding(Source.class)
	public static class SourceFromSupplier {
		@Bean
		public Supplier<Date> date() {
			return () -> new Date();
		}
	}

	@EnableAutoConfiguration
	@EnableBinding(Processor.class)
	public static class ProcessorFromFunction {
		@Bean
		public Function<String, String> toUpperCase() {
			return s -> s.toUpperCase();
		}
	}

	@EnableAutoConfiguration
	@EnableBinding(Sink.class)
	public static class SinkFromConsumer {
		@Bean
		public Consumer<String> sink() {
			return s -> System.out.println(s);
		}
	}

	@EnableAutoConfiguration
//	@EnableBinding(Sink.class)
	public static class SinkFromConsumerNoEnableBinding {
		@Bean
		public Consumer<String> sink() {
			return s -> System.out.println("==> " + s);
		}
	}
}

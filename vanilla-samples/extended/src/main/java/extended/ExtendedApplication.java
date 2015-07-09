package extended;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.streams.EnableChannelBinding;
import org.springframework.cloud.streams.aggregate.AggregateBuilder;
import org.springframework.cloud.streams.aggregate.AggregateConfigurer;

import sink.LogSink;
import source.TimeSource;
import transform.LoggingTransformer;

@SpringBootApplication
@EnableChannelBinding
public class ExtendedApplication implements AggregateConfigurer {

	@Override
	public void configure(AggregateBuilder builder) {
		// @formatter:off
		builder
		.from(TimeSource.class).as("source")
		.via(LoggingTransformer.class)
		.via(LoggingTransformer.class).profiles("other")
		.to(LogSink.class);
		// @formatter:on
	}

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(ExtendedApplication.class, args);
	}

}

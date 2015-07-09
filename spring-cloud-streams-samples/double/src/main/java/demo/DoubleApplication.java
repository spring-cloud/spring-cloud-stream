package demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.streams.aggregate.AggregateBuilder;
import org.springframework.cloud.streams.aggregate.AggregateConfigurer;

import config.SinkModuleDefinition;
import config.SourceModuleDefinition;

@SpringBootApplication
public class DoubleApplication implements AggregateConfigurer {

	@Override
	public void configure(AggregateBuilder builder) {
		builder.from(SourceModuleDefinition.class).as("source")
		.to(SinkModuleDefinition.class).as("sink");
	}

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(DoubleApplication.class, args);
	}

}

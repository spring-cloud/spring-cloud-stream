package sample.consumer;

import java.util.function.Consumer;

import com.example.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(proxyBeanMethods = false)
public class ConfluentAvroConsumerApplication {

	private final Logger logger = LoggerFactory.getLogger(ConfluentAvroConsumerApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(ConfluentAvroConsumerApplication.class, args);
	}

	@Bean
	Consumer<Sensor> process()  {
		return input -> logger.info("input: {}", input);
	}

}

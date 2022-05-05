package sample.consumer;

import java.util.function.Consumer;

import com.example.Sensor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ConfluentAvroConsumerApplication {

	private final Log logger = LogFactory.getLog(getClass());

	public static void main(String[] args) {
		SpringApplication.run(ConfluentAvroConsumerApplication.class, args);
	}

	@Bean
	public Consumer<Sensor> process()  {
		return input -> logger.info("input: " + input);
	}

}

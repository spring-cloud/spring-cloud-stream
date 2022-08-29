package sample.producer2;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.schema.registry.client.EnableSchemaRegistryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import com.example.Sensor;

@SpringBootApplication(proxyBeanMethods = false)
@EnableSchemaRegistryClient
@RestController
public class Producer2ApplicationKafka {

	private Random random = new Random();

	BlockingQueue<Sensor> unbounded = new LinkedBlockingQueue<>();

	public static void main(String[] args) {
		SpringApplication.run(Producer2ApplicationKafka.class, args);
	}

	@RequestMapping(value = "/messages", method = RequestMethod.POST)
	public String sendMessage() {
		unbounded.offer(randomSensor());
		return "ok, have fun with v2 payload!";
	}

	@Bean
	public Supplier<Sensor> supplier() {
		return () -> unbounded.poll();
	}

	private Sensor randomSensor() {
		Sensor sensor = new Sensor();
		sensor.setId(UUID.randomUUID().toString() + "-v2");
		sensor.setAcceleration(random.nextFloat() * 10);
		sensor.setVelocity(random.nextFloat() * 100);
		sensor.setInternalTemperature(random.nextFloat() * 50);
		sensor.setExternalTemperature(random.nextFloat() * 50);
		sensor.setAccelerometer(null);
		sensor.setMagneticField(null);
		return sensor;
	}
}


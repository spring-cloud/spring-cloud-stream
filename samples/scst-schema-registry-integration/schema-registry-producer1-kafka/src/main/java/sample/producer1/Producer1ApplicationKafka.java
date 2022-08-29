package sample.producer1;

import java.util.Random;
import java.util.UUID;

import com.example.Sensor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.cloud.stream.schema.registry.client.EnableSchemaRegistryClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication(proxyBeanMethods = false)
@EnableSchemaRegistryClient
@RestController
public class Producer1ApplicationKafka {

	private Random random = new Random();

	@Autowired
	StreamBridge streamBridge;

	public static void main(String[] args) {
		SpringApplication.run(Producer1ApplicationKafka.class, args);
	}

	private Sensor randomSensor() {
		Sensor sensor = new Sensor();
		sensor.setId(UUID.randomUUID() + "-v1");
		sensor.setAcceleration(random.nextFloat() * 10);
		sensor.setVelocity(random.nextFloat() * 100);
		sensor.setTemperature(random.nextFloat() * 50);
		return sensor;
	}

	@RequestMapping(value = "/messages", method = RequestMethod.POST)
	public String sendMessage() {
		streamBridge.send("supplier-out-0", randomSensor());
		return "ok, have fun with v1 payload!";
	}

}


package demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.streams.EnableMessageBus;
import org.springframework.context.annotation.ComponentScan;

import config.ModuleDefinition;

@SpringBootApplication
@EnableMessageBus
@ComponentScan(basePackageClasses=ModuleDefinition.class)
public class TapApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(TapApplication.class, args);
	}

}

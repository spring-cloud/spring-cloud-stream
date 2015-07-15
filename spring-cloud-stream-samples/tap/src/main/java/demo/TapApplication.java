package demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableModule;
import org.springframework.context.annotation.ComponentScan;

import config.TappingLoggingSink;

@SpringBootApplication
@EnableModule
@ComponentScan(basePackageClasses= TappingLoggingSink.class)
public class TapApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(TapApplication.class, args);
	}

}

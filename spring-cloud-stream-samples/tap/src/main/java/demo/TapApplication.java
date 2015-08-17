package demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import config.TappingLoggingSink;

@SpringBootApplication
@ComponentScan(basePackageClasses= TappingLoggingSink.class)
public class TapApplication {

	public static void main(String[] args) {
		SpringApplication.run(TapApplication.class, args);
	}

}

package demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import transform.LoggingTransformer;

@SpringBootApplication
@ComponentScan(basePackageClasses=LoggingTransformer.class)
public class TransformApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(TransformApplication.class, args);
	}

}

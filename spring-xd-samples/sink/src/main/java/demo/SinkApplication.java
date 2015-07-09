package demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.streams.annotation.EnableModule;
import org.springframework.context.annotation.ComponentScan;

import config.LoggerSink;

@SpringBootApplication
@ComponentScan(basePackageClasses = LoggerSink.class)
public class SinkApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(SinkApplication.class, args);
	}

}

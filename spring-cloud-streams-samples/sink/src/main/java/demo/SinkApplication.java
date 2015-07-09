package demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import sink.LogSink;

@SpringBootApplication
@ComponentScan(basePackageClasses=LogSink.class)
public class SinkApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(SinkApplication.class, args);
	}

}

# Spring Integration: Messaging as a Microservice

This is an experimental project allowing a user to develop and run messaging microservices using Spring Integration and run them locally or in the cloud, or even on Spring XD. It also allows a user to develop and run an XD module locally. Just create `MessageChannels` "input" and/or "output" and add `@EnableMessageBus` and run your app as a Spring Boot app (single application context).  You just need to connect to the physical broker for the bus, which is automatic if the relevant bus implementation is available on the classpath. The sample uses Redis.

Here's a sample source module (output channel only):

```
@SpringBootApplication
@EnableMessageBus
@ComponentScan(basePackageClasses=ModuleDefinition.class)
public class ModuleApplication {

  public static void main(String[] args) throws InterruptedException {
    SpringApplication.run(ModuleApplication.class, args);
  }

}

@Configuration
public class ModuleDefinition {

  @Value("${format}")
  private String format;

  @Bean
  public MessageChannel output() {
    return new DirectChannel();
  }

  @Bean
  @InboundChannelAdapter(value = "output", autoStartup = "false", poller = @Poller(fixedDelay = "${fixedDelay}", maxMessagesPerPoll = "1"))
  public MessageSource<String> timerMessageSource() {
    return () -> new GenericMessage<>(new SimpleDateFormat(format).format(new Date()));
  }

}
```

The `bootstrap.yml` has the module group (a.k.a. stream name in XD), name and index, e.g.

```
---
spring:
  bus:
    group: testtock
    name: ${spring.application.name:ticker}
    index: 0 # source
```

To be deployable as an XD module in a "traditional" way you need `/config/*.properties` to point to any available Java config classes (via `base_packages` or `options_class`), or else you can put traditional XML configuration in `/config/*.xml`. You don't need those things to run as a consumer or producer to an existing XD system. There's an XML version of the same sample (a "timer" source).

## XD Module Samples

There are several samples, all running on the redis transport (so you need redis running locally to test them):

* `source` is a Java config version of the classic "timer" module from Spring XD. It has a "fixedDelay" option (in milliseconds) for the period between emitting messages.

* `sink` is a Java config version of the classic "log" module from Spring XD. It has no options (but some could easily be added), and just logs incoming messages at INFO level.

* `tap` is the same as the sink sample, except it is configured to tap the source sample output. When it is running it looks a lot like the sink, except that it only gets copies of the messages in the broker, and since it is a pub-sub subscriber, it only gets the messages sent since it started.

* `source-xml` is a copy of the classic "timer" module from Spring XD.

If you run the source and the sink and point them at the same redis instance (e.g. do nothing to get the one on localhost, or the one they are both bound to as a service on Cloud Foundry) then they will form a "stream" and start talking to each other. All the samples have friendly JMX and Actuator endpoints for inspecting what is going on in the system.

## Module or App

Code using this library can be deployed as a standalone app or as an XD module. In standalone mode you app will run happily as a service or in any PaaS (Cloud Foundry, Lattice, Heroku, Azure, etc.). Depending on whether your main aim is to develop an XD module and you just want to test it locally using the standalone mode, or if the ultimate goal is a standalone app, there are some things that you might do differently.

### Module Options

Module option (placeholders) default values can be set in `/config/*.properties` as per a normal XD module, and they can be overridden at runtime in standalone mode using standard Spring Boot configuration (e.g. `application.yml`). Because of the way XD likes to organize options, the default values can also be set as `option.*` in `bootstrap.yml` (in standalone mode) or as System properties (generally).

### Local Configuration

The `application.yml` and `bootstrap.yml` files are ignored by XD when deploying the module natively, so you can put whatever you like in there to control the app in standlone mode.

### Fat JAR

You can run in standalone mode from your IDE for testing. To run in production you can create an executable (or "fat") JAR using the standard Spring Boot tooling, but the executable JAR has a load of stuff in it that isn't needed if it's going to be deployed as an XD module. In that case you are better off with the normal JAR packaging provided by Maven or Gradle.

## Making Standalone Modules Talk to Each Other

The "group" and "index" are used to create physical endpoints in the external broker (e.g. `queue.<group>.<index>` in Redis), so a source (output only) has `index=0` (the default) and downstream modules have the same group but incremented index, with a sink module (input only) having the highest index. To listen to the output from an existing app, just use the same "group" name and an index 1 larger than the app before it in the chain. The index can be anything, as long as successive modules have consecutive values. 

> Note: since the same naming conventions are used in XD, you can spy on or send messages to an existing XD stream by copying the stream name (to `spring.bus.group`) and knowing the index of the XD module you want to interact with.

## Taps

All output channels are also tapped by default so you can also attach a module to a pub-sub endpoint and listen to the tap if you know the module metadata (e.g. `topic.tap:stream:<name>.<group>.<index>` in Redis). To tap an existing output channel you just need to know its group, name and index, e.g.

```
spring:
  bus:
    group: tocktap
    name: logger
    index: 0
    tap:
      group: testtock
      name: ticker
      index: 0
```

The `spring.bus.tap` section tells the module runner which topic you want to subscribe to. It creates a new group (a tap can't be in the same group as the one it is tapping) and starts a new index count, in case anyone wants to listen downstream.
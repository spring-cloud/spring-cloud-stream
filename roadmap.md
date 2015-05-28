# Messaging Microservices

Messaging and asynchronous patterns are completely natural with microservices, but a lot of the start of the art material concentrates on HTTP, JSON and REST. This document is about "Message-driven Microservices" with Spring. It tracks the convergence of various ideas that are floating around in Spring Cloud, Spring Boot and Spring XD.

There is already a [`spring-xd-module-runner` project](https://github.com/dsyer/spring-xd-module-runner) where we started experimenting with allowing user to develop and run an XD module locally. We can extrapolate from there to a more flexible model that leads optionally to a deployable XD module, but can also be used to form more flexible structures than a simple "stream".

## Basic Programming Model

Just create `MessageChannels` "input" and/or "output" and add `@EnableMessageBus` and run your app as a Spring Boot app (single application context).  You need to connect to the physical broker for the bus, which is automatic if the relevant bus implementation is available on the classpath. The sample uses Redis.

Here's a sample source module (output channel only):

```
@SpringBootApplication
@EnableMessageBus
@ComponentScan(basePackageClasses=ModuleDefinition.class)
public class MessageBusApplication {

  public static void main(String[] args) {
    SpringApplication.run(MessageBusApplication.class, args);
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

The `bootstrap.yml` has the module group (a.k.a. stream name), name and index, e.g.

```
---
spring:
  bus:
    group: testtock
    name: ${spring.application.name:ticker}
    index: 0 # source
```

## Richer Input and Output

A generic message-driven microservice can have more than 2 inputs and outputs. For example if it contains a router component, then it might want to send messages downstream to 2 (or more) completely different classes of consumers. The number and purpose of those downstream channels might even change at runtime.

Spring XD has the concept of a "stream" where modules pipe their output into the next module's input (like a UN\*X shell). This is effectively an opinionated, special-case implementation of the more general message-driven microservice, and in it should be possible to re-architect Spring XD so that it still adds value but builds on top of a framework that is more general.

Spring XD actually already supports arbitrary graphs of input and output via named channels. One of the aims of this project is to make that easier to program (or declare), and to make modules composable into a self-contained application.

> NOTE: For the sake of clarity, consider the example of an XD module that contains a router, sending each message to one of two destinations (for invalid or valid messages respectively). For XD purposes this module is a "sink" because it has input but no uniq output channel. The output is handled and redirected through the Bus using a `MessageChannelResolver` that is driven by naming convention (destinations start with "queue:" or "topic:" according to their pubsub semantics). Users can create streams that listen to those channels by using the same names.

## Deployment as an XD Module

To be deployable as an XD module in a "traditional" way you need `/config/*.properties` to point to any available Java config classes (via `base_packages` or `options_class`), or else you can put traditional XML configuration in `/config/*.xml`. You don't need those things to run as a consumer or producer to an existing XD system. The `spring-xd-module-runner` library uses a Spring Cloud bootstrap context to initialize the `Module` environment properties in a way that simulates being deployed in a "full" XD system.

## Backlog

- [ ] Support for multiple input and output channels

- [ ] Support for pubsub as "primary" input/output (in addition to the existing queue semantics)

- [ ] Support for more than one `MessageBus` (e.g. local and redis) in the same app

- [ ] Correlation and message tracing

- [ ] Extract `spring-xd-dirt` dependencies into a separate module

- [ ] Re-use existing XD modules as libraries

- [ ] Re-use existing XD analytics as libraries (possibly attempt merge with Spring Boot metrics)

## Barriers to Progress

The best plan for making progress, where we keep in sight the goal of eventually having Spring XD converge with this project, is to shadow Spring XD andtry and extract as much goodness from it as we can. The `MessageBus` is really the core concept and it is already largely split out.

- [ ] Spring XD package names should eventually be removed as messaging is a lower level concern.

- [ ] Ditto configuration properties in `xd.*` should be moved to `spring.bus.*` (or something).

- [ ] We need the XML configuration from `/META-INF/spring-xd/bus/**` and `/META-INF/spring-xd/analytics` but

  - [ ] There are no defaults for several properties in `xd.messagebus.*` so applications have to have a load of boilerplate configuration in `application.yml`

  - [ ] The XML is in `spring-xd-dirt` which we don't want to depend on. Maybe it should be in the messagebus impementation jars?

  - [x] Do we need the analytics configuration? It should at least be optional probably. Answer "no".

- [ ] The `spring-xd-dirt` library contains some of the primitives we might need, especially when building the bridge to create XD modules as apps. It would be best if they could be extracted into another library.

  - [ ] `MessageBusAwareChannelResolver` and `MessageBusAwareRouterBeanPostProcessor` seem like they should be pulled out of dirt.

  - [x] A `ModuleDefinition` is only needed to initialize options for an XD module (so not really needed for the general case). We can split the module options initializer outr into a separate module that you only need if you know you want to test an XD module with its native options meatdata.

  - [ ] `XdHeaders` (e.g. for history)

- [ ] There is a curator dependency in Spring XD that can't be shaken off.

- [ ] Spring XD plugins provide a rich set of lifecycle hooks, but those would not all be needed and are an awkward mismatch with a "pure-play" Spring Boot approach, where the application is either running or not.

- [ ] Spring XD Module options are a prime example of the above. The `StreamPlugin` is responsible for setting up default values for "options" which are really nothing more than property sources in the `Environment` (at least from the point of view of the ideal developer experience). Some of the code from that was used in a Spring Cloud bootstrap context in the `spring-xd-module-runner` project, which puts it in the right part of the lifecycle. It might be better to try and wean XD off its native "options" and back onto a model based on vanilla Spring Boot and Spring Cloud features (`@ConfigurationProperties` and the bootstrap context for example).

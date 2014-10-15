package org.springframework.cloud.bus.turbine;

import com.netflix.turbine.aggregator.InstanceKey;
import com.netflix.turbine.aggregator.StreamAggregator;
import com.netflix.turbine.internal.JsonUtility;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.text.sse.ServerSentEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
//import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.Map;

import static io.reactivex.netty.pipeline.PipelineConfigurators.*;

/**
 * @author Spencer Gibb
 */
@Configuration
@EnableAutoConfiguration
//@EnableEurekaClient
@Slf4j
public class StartBusTurbine implements SmartLifecycle {

    private boolean running = false;

    @Autowired
    Environment env;

    @Bean
    public PublishSubject<Map<String, Object>> hystrixSubject() {
        return PublishSubject.create();
    }

    @Bean
    public HttpServer<ByteBuf, ServerSentEvent> aggregatorServer() {
        // multicast so multiple concurrent subscribers get the same stream
        Observable<Map<String, Object>> publishedStreams = StreamAggregator.aggregateGroupedStreams(hystrixSubject()
                    .groupBy(data -> InstanceKey.create((String) data.get("instanceId"))))
                .doOnUnsubscribe(() -> log.info("BusTurbine => Unsubscribing aggregation."))
                .doOnSubscribe(() -> log.info("BusTurbine => Starting aggregation"))
                .flatMap(o -> o).publish().refCount();

        int port = new Integer(env.getProperty("server.port", "8989"));
        HttpServer<ByteBuf, ServerSentEvent> httpServer = RxNetty.createHttpServer(port, (request, response) -> {
            StartBusTurbine.this.log.info("BusTurbine => SSE Request Received");
            response.getHeaders().setHeader("Content-Type", "text/event-stream");
            return publishedStreams
                    .doOnUnsubscribe(() -> log.info("BusTurbine => Unsubscribing RxNetty server connection"))
                    .flatMap(data -> response.writeAndFlush(new ServerSentEvent(null, null, JsonUtility.mapToJson(data))));
        }, sseServerConfigurator());
        return httpServer;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public void start() {
        aggregatorServer().start();
    }

    @Override
    public void stop() {
        try {
            aggregatorServer().shutdown();
        } catch (InterruptedException e) {
            log.error("Error shutting down", e);
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder().web(false).sources(StartBusTurbine.class).run(args);
    }
}

package org.springframework.cloud.bus.turbine;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.cloud.bus.Constants;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Spencer Gibb
 */
@Configuration
@ConditionalOnClass(AmqpTemplate.class)
public class TurbineAutoConfiguration {

    @Configuration
    @ConditionalOnExpression("${hystrix.stream.bus.turbine.enabled:true}")
    protected static class HystrixStreamAggregatorAutoConfiguration {

        @Autowired
        private ConnectionFactory connectionFactory;

        @Autowired
        private AmqpAdmin amqpAdmin;

        //TODO: how to fail gracefully if no rabbit?
        @Bean
        public DirectExchange hystrixStreamExchange() {
            DirectExchange exchange = new DirectExchange(Constants.HYSTRIX_STREAM_NAME);
            amqpAdmin.declareExchange(exchange);
            return exchange;
        }

        @Bean
        public Queue hystrixStreamQueue() {
            Map<String, Object> args = new HashMap<>();
            args.put("x-message-ttl", 60000); //TODO: configure TTL
            Queue queue = new Queue(Constants.HYSTRIX_STREAM_NAME, false, false, false, args);
            amqpAdmin.declareQueue(queue);
            amqpAdmin.declareBinding(BindingBuilder.bind(queue).to(hystrixStreamExchange()).with(""));
            return queue;
        }

        @Bean
        public IntegrationFlow hystrixStreamAggregatorInboundFlow() {
            return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, hystrixStreamQueue()))
                    .channel("hystrixStreamAggregator")
                    .get();
        }

        @Bean
        public Aggregator hystrixStreamAggregator() {
            return new Aggregator();
        }
    }


}

package org.springframework.cloud.bus.hystrix;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.cloud.bus.Constants;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author Spencer Gibb
 */
@Configuration
@ConditionalOnClass(AmqpTemplate.class)
public class HystrixStreamAutoConfiguration {
    @Configuration
    @ConditionalOnExpression("${hystrix.stream.bus.enabled:false}")
    @IntegrationComponentScan(basePackageClasses = HystrixStreamChannel.class)
    @EnableScheduling
    protected static class HystrixStreamBusAutoConfiguration {

        @Autowired
        private AmqpTemplate amqpTemplate;

        @Bean
        public HystrixStreamTask hystrixStreamTask() {
            return new HystrixStreamTask();
        }

        @Bean
        public DirectChannel hystrixStream() {
            return new DirectChannel();
        }
        
        @Bean
        public DirectExchange hystrixStreamExchange() {
            DirectExchange exchange = new DirectExchange(Constants.HYSTRIX_STREAM_NAME);
            return exchange;
        }

        @ConditionalOnExpression("${hystrix.stream.bus.enabled:true}")
        @Bean
        public IntegrationFlow hystrixStreamOutboundFlow() {
            return IntegrationFlows.from("hystrixStream")
                    //TODO: set content type
                    /*.enrichHeaders(new ComponentConfigurer<HeaderEnricherSpec>() {
                        @Override
                        public void configure(HeaderEnricherSpec spec) {
                            spec.header("content-type", "application/json", true);
                        }
                    })*/
                    .handle(Amqp.outboundAdapter(this.amqpTemplate).exchangeName(Constants.HYSTRIX_STREAM_NAME))
                    .get();
        }

        /*@Bean
        public DirectChannel wiretapChannel() {
            return MessageChannels.direct().get();
        }

        @Bean
        @GlobalChannelInterceptor(patterns = "hystrixStreamOutboundFlow*")
        public WireTap wireTap() {
            return new WireTap(wiretapChannel());
        }

        @Bean
        public IntegrationFlow loggingFlow() {
            LoggingHandler handler = new LoggingHandler("INFO");
            handler.setShouldLogFullMessage(true);
            return IntegrationFlows.from(wiretapChannel())
                    .handle(handler)
                    .get();
        }*/
    }

}

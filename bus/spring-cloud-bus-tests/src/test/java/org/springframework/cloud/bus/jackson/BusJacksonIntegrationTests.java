package org.springframework.cloud.bus.jackson;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT,
        properties = "spring.jackson.serialization.WRITE_DATES_AS_TIMESTAMPS:true")
public class BusJacksonIntegrationTests {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private BusJacksonMessageConverter converter;

    @Test
	@SuppressWarnings("unchecked")
    public void testCustomEventSerializes() {
        assertThat(converter.isMapperCreated()).isFalse();

        // set by configuration
        assertThat(converter.getMapper().getSerializationConfig()
                .isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)).isTrue();

        Map map = rest.getForObject("http://localhost:" + port + "/date", Map.class);
        assertThat(map).containsOnlyKeys("date");
        assertThat(map.get("date")).isInstanceOf(Long.class);

        rest.put("http://localhost:"+port+"/names"+"/foo", null);
        rest.put("http://localhost:"+port+"/names"+"/bar", null);

        ResponseEntity<List> response = rest.getForEntity("http://localhost:" + port + "/names", List.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("foo", "bar");
    }

    public static class NameEvent extends RemoteApplicationEvent {

        private String name;

        protected NameEvent() {}

        public NameEvent(Object source, String originService, String name) {
            super(source, originService);
            this.name = name;
        }

        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }

    }

    @RestController
    @EnableAutoConfiguration
    @SpringBootConfiguration
    @RemoteApplicationEventScan
    protected static class Config {
        final private Set<String> names = ConcurrentHashMap.newKeySet();
        @Autowired
        private ServiceMatcher busServiceMatcher;
        @Autowired
        private ApplicationEventPublisher publisher;

        @GetMapping("/names")
        public Collection<String> names() {
            return this.names;
        }

        @PutMapping("/names/{name}")
        public void sayName(@PathVariable String name) {
            this.names.add(name);
            publisher.publishEvent(new NameEvent(this, busServiceMatcher.getServiceId(), name));
        }

        @GetMapping("/date")
        public Map<String, Object> testTimeJsonSerialization(){
            Map<String, Object> map = new HashMap<>();
            map.put("date", new Date());
            return map;
        }

        @EventListener
        public void handleNameSaid(NameEvent event) {
            this.names.add(event.getName());
        }

    }
}

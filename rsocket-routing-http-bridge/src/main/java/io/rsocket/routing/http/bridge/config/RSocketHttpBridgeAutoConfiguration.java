package io.rsocket.routing.http.bridge.config;

import java.util.function.Function;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.http.bridge.core.HttpRSocketFunction;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

/**
 * @author Olga Maciaszek-Sharma
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(value = "spring.cloud.rsocket.routing.http.bridge.enabled", matchIfMissing = true)
// TODO: make function deps optional + add conditional
public class RSocketHttpBridgeAutoConfiguration {

	@Autowired
	RoutingRSocketRequester requester;

	@Bean
	public Function<Mono<Message<String>>, Mono<Message<String>>> function() {
		return new HttpRSocketFunction(requester);
	}

}

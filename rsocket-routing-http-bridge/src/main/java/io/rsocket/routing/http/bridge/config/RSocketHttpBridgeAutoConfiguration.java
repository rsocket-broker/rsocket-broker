package io.rsocket.routing.http.bridge.config;

import java.util.function.Function;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.http.bridge.core.FireAndForgetFunction;
import io.rsocket.routing.http.bridge.core.RequestChannelFunction;
import io.rsocket.routing.http.bridge.core.RequestResponseFunction;
import io.rsocket.routing.http.bridge.core.RequestStreamFunction;
import reactor.core.publisher.Flux;
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

	// Stay with four different endpoints or switch to some other way of differentiating?

	@Bean
	public Function<Mono<Message<String>>, Mono<Message<String>>> rr() {
		return new RequestResponseFunction(requester);
	}

	@Bean
	public Function<Flux<Message<String>>, Flux<Message<String>>> rc() {
		return new RequestChannelFunction(requester);
	}

	@Bean
	public Function<Mono<Message<String>>, Flux<Message<String>>> rs() {
		return new RequestStreamFunction(requester);
	}

	@Bean
	public Function<Mono<Message<String>>, Mono<Void>> ff() {
		return new FireAndForgetFunction(requester);
	}

}

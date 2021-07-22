package io.rsocket.routing.http.bridge.config;

import java.util.function.Function;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.client.spring.RoutingRSocketRequesterBuilder;
import io.rsocket.routing.common.spring.ClientTransportFactory;
import io.rsocket.routing.http.bridge.core.FireAndForgetFunction;
import io.rsocket.routing.http.bridge.core.RequestChannelFunction;
import io.rsocket.routing.http.bridge.core.RequestResponseFunction;
import io.rsocket.routing.http.bridge.core.RequestStreamFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

/**
 * @author Olga Maciaszek-Sharma
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(value = "spring.cloud.rsocket.routing.http.bridge.enabled", matchIfMissing = true)
@EnableConfigurationProperties(RSocketHttpBridgeProperties.class)
public class RSocketHttpBridgeAutoConfiguration implements ApplicationContextAware, InitializingBean {

	private final RoutingRSocketRequester requester;
	private final RSocketHttpBridgeProperties properties;
	private final RoutingRSocketRequesterBuilder requesterBuilder;
	private final ObjectProvider<ClientTransportFactory> transportFactories;
	private ConfigurableApplicationContext applicationContext;

	public RSocketHttpBridgeAutoConfiguration(RoutingRSocketRequester requester, RSocketHttpBridgeProperties properties, RoutingRSocketRequesterBuilder requesterBuilder,
			ObjectProvider<ClientTransportFactory> transportFactories) {
		this.requester = requester;
		this.properties = properties;
		this.requesterBuilder = requesterBuilder;
		this.transportFactories = transportFactories;
	}

	// Stay with four different endpoints or switch to some other way of differentiating?

	@Bean
	public Function<Mono<Message<Byte[]>>, Mono<Message<Byte[]>>> rr() {
		return new RequestResponseFunction(requesterBuilder, requester, transportFactories, properties);
	}

	@Bean
	public Function<Flux<Message<Byte[]>>, Flux<Message<Byte[]>>> rc() {
		return new RequestChannelFunction(requesterBuilder, requester, transportFactories, properties);
	}

	@Bean
	public Function<Mono<Message<Byte[]>>, Flux<Message<Byte[]>>> rs() {
		return new RequestStreamFunction(requesterBuilder, requester, transportFactories, properties);
	}

	@Bean
	public Function<Mono<Message<Byte[]>>, Mono<Void>> ff() {
		return new FireAndForgetFunction(requesterBuilder, requester, transportFactories, properties);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		FunctionProperties functionProperties = applicationContext
				.getBean(FunctionProperties.class);
		String definition = functionProperties.getDefinition();
		if (definition == null && properties.isRequestResponseDefault()) {
			functionProperties.setDefinition("rr");
		}
	}
}

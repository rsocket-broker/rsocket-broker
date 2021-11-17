/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.routing.broker.spring.cluster;

import io.rsocket.routing.broker.spring.BrokerProperties;
import io.rsocket.routing.broker.spring.BrokerProperties.Broker;
import reactor.core.publisher.Sinks;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

/**
 * Once a broker node has started, this class passes configured brokers on for connection management.
 */
public class PropertiesClusterNodeProvider implements ApplicationListener<ApplicationReadyEvent> {

	private final BrokerProperties properties;
	private final Sinks.Many<Broker> connectionEventPublisher;

	public PropertiesClusterNodeProvider(BrokerProperties properties, Sinks.Many<Broker> connectionEventPublisher) {
		this.properties = properties;
		this.connectionEventPublisher = connectionEventPublisher;
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		// TODO: tags
		for (Broker broker : properties.getBrokers()) {
			connectionEventPublisher.tryEmitNext(broker);
		}
	}

}

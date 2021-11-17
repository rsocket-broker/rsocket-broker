/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.broker.http.bridge.core;

import java.time.Duration;
import java.util.function.Function;

import io.rsocket.broker.client.spring.BrokerRSocketRequester;
import io.rsocket.broker.common.spring.ClientTransportFactory;
import io.rsocket.broker.http.bridge.config.RSocketHttpBridgeProperties;
import org.apache.commons.logging.Log;

import org.springframework.beans.factory.ObjectProvider;

import static org.apache.commons.logging.LogFactory.getLog;

/**
 * Base class for HTTP-RSocket bridge functions.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
abstract class AbstractHttpRSocketFunction<I, O> implements Function<I, O> {

	protected final Log LOG = getLog(getClass());

	protected BrokerRSocketRequester requester;
	ObjectProvider<ClientTransportFactory> transportFactories;
	protected final RSocketHttpBridgeProperties properties;
	protected final Duration timeout;

	protected AbstractHttpRSocketFunction(BrokerRSocketRequester requester, ObjectProvider<ClientTransportFactory> transportFactories,
			RSocketHttpBridgeProperties properties) {
		this.properties = properties;
		timeout = properties.getTimeout();
		this.transportFactories = transportFactories;
		this.requester = requester;
	}

	protected void logTimeout(String address, String route) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String
					.format("Timeout occurred while retrieving RSocket response from address: %s, route: %s. Response was not retrieved within %s", address, route, timeout));
		}
	}

	protected void logException(Throwable error, String address, String route) {
		if (LOG.isErrorEnabled())
			LOG.error(String
					.format("Exception occurred while retrieving RSocket response from address: %s, route: %s", address, route), error);
	}

}

/*
 * Copyright 2020 the original author or authors.
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

package io.rsocket.routing.broker.spring.cluster;

import io.rsocket.routing.frames.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.annotation.ConnectMapping;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ClusterController {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public ClusterController() {
		System.out.println();
	}

	@ConnectMapping
	public void onConnect(BrokerInfo brokerInfo, RSocketRequester rSocketRequester) {
		brokerInfo(brokerInfo, rSocketRequester);
	}

	@RequestMapping("cluster.broker-info")
	public boolean brokerInfo(BrokerInfo brokerInfo, RSocketRequester rSocketRequester) {
		logger.info("received connection from {}", brokerInfo);
		// TODO: store broker info
		// TODO: send BrokerInfo back
		return false;
	}

	@MessageMapping("hello")
	public Mono<String> hello(String name) {
		return Mono.just("Hello " + name);
	}

}

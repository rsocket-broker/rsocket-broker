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

package io.rsocket.routing.broker.spring.test;

import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.common.Id;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {"io.rsocket.routing.broker.broker-id=00000000-0000-0000-0000-000000000007",
		"io.rsocket.routing.broker.brokers[0].host=myhost",
		"io.rsocket.routing.broker.brokers[0].port=99"})
public class BrokerPropertiesTests {

	@Autowired
	BrokerProperties properties;

	@Test
	public void idWorksAsProperty() {
		assertThat(properties.getBrokerId()).isEqualTo(Id.from("00000000-0000-0000-0000-000000000007"));
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	protected static class TestConfig {

	}
}

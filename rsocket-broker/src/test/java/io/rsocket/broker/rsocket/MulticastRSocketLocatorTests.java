/*
 * Copyright 2021 the original author or authors.
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

package io.rsocket.broker.rsocket;

import io.rsocket.broker.query.RSocketQuery;
import io.rsocket.broker.common.Id;
import io.rsocket.broker.common.WellKnownKey;
import io.rsocket.broker.frames.Address;
import io.rsocket.broker.frames.RoutingType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class MulticastRSocketLocatorTests {

	@Mock
	private RSocketQuery rSocketQuery;

	@Test
	public void locateWorks() {
		Address address = Address.from(Id.random()).with(WellKnownKey.SERVICE_NAME, "service1")
				.routingType(RoutingType.MULTICAST).build();

		MulticastRSocketLocator locator = new MulticastRSocketLocator(rSocketQuery);

		assertThat(locator.supports(RoutingType.MULTICAST)).isTrue();

		assertThat(locator.locate(address)).isInstanceOf(MulticastRSocket.class);
	}

}

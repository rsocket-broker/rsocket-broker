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

import java.util.Arrays;

import io.rsocket.RSocket;
import io.rsocket.broker.common.Id;
import io.rsocket.broker.common.WellKnownKey;
import io.rsocket.broker.frames.Address;
import io.rsocket.broker.frames.RoutingType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CompositeRSocketLocatorTests {

	@Mock
	private RSocketLocator locator1;

	@Mock
	private RSocketLocator locator2;

	@Test
	public void compositeWorks() {
		Address address = Address.from(Id.random()).with(WellKnownKey.SERVICE_NAME, "service1")
				.routingType(RoutingType.MULTICAST).build();

		CompositeRSocketLocator composite = new CompositeRSocketLocator(Arrays
				.asList(locator1, locator2));

		when(locator2.supports(RoutingType.MULTICAST)).thenReturn(true);
		when(locator2.locate(address)).thenReturn(mock(RSocket.class));

		assertThat(composite.supports(RoutingType.MULTICAST)).isTrue();

		assertThat(composite.locate(address)).isNotNull();
	}


	@Test
	public void noLocatorForRoutingThrowsException() {
		Address address = Address.from(Id.random()).with(WellKnownKey.SERVICE_NAME, "service1").build();

		CompositeRSocketLocator composite = new CompositeRSocketLocator(Arrays
				.asList(locator1, locator2));

		assertThatThrownBy(() -> composite.locate(address)).isInstanceOf(IllegalStateException.class);
	}
}

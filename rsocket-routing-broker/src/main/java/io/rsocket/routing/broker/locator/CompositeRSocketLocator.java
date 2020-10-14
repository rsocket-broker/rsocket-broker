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

package io.rsocket.routing.broker.locator;

import java.util.List;

import io.rsocket.RSocket;
import io.rsocket.routing.frames.Address;

/**
 * Composite RSocketLocator. The provided RSocketLocator instances should be in priority sorted order, highest priority first.
 */
public class CompositeRSocketLocator implements RSocketLocator {

	private final List<RSocketLocator> locators;

	public CompositeRSocketLocator(List<RSocketLocator> locators) {
		this.locators = locators;
	}

	@Override
	public boolean supports(Address.RoutingType routingType) {
		for (RSocketLocator RSocketLocator : locators) {
			if (RSocketLocator.supports(routingType)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public RSocket locate(Address address) {
		for (RSocketLocator locator : locators) {
			if (locator.supports(address.getRoutingType())) {
				return locator.locate(address);
			}
		}
		throw new IllegalStateException("No RSocketLocator for RoutingType " + address.getRoutingType());
	}
}

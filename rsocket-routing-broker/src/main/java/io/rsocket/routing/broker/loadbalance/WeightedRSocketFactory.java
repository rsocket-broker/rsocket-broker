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

package io.rsocket.routing.broker.loadbalance;

import java.util.Objects;
import java.util.function.Function;

import io.rsocket.RSocket;
import io.rsocket.routing.broker.stats.FrugalQuantile;
import io.rsocket.routing.broker.stats.Quantile;

public class WeightedRSocketFactory implements Function<RSocket, RSocket> {

	//TODO: configurable defaults
	private final Quantile lowerQuantile = new FrugalQuantile(WeightedRSocket.DEFAULT_LOWER_QUANTILE);
	private final Quantile higherQuantile = new FrugalQuantile(WeightedRSocket.DEFAULT_HIGHER_QUANTILE);

	@Override
	public RSocket apply(RSocket rSocket) {
		Objects.requireNonNull(rSocket, "rSocket may not be null");
		if (rSocket instanceof WeightedRSocket) {
			return rSocket;
		}
		return new WeightedRSocket(rSocket, lowerQuantile, higherQuantile);
	}
}

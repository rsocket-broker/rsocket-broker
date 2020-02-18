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

import java.util.List;
import java.util.SplittableRandom;

import io.rsocket.RSocket;
import io.rsocket.routing.broker.stats.FrugalQuantile;
import io.rsocket.routing.broker.stats.Quantile;
import io.rsocket.routing.common.Tags;
import reactor.core.publisher.Mono;

public class WeightedLoadBalancer implements LoadBalancer {
	private static final ThreadLocal<SplittableRandom> localSplittableRandom =
			ThreadLocal.withInitial(SplittableRandom::new);

	//TODO: configurable defaults
	private final Quantile lowerQuantile = new FrugalQuantile(WeightedRSocket.DEFAULT_LOWER_QUANTILE);
	private final Quantile higherQuantile = new FrugalQuantile(WeightedRSocket.DEFAULT_HIGHER_QUANTILE);
	public static final double DEFAULT_EXP_FACTOR = 4.0;
	public static final int EFFORT = 5;


	@Override
	public Mono<Response> choose(Request request) {
		// TODO: get expFactor and quantiles from request?
		final double expFactor = DEFAULT_EXP_FACTOR;

		List<RSocket> rSockets = request.getRSockets();

		WeightedRSocket rSocket;
		SplittableRandom rnd = localSplittableRandom.get();
		int size = rSockets.size();
		if (size == 1) {
			rSocket = (WeightedRSocket) rSockets.get(0);
		}
		else if (size > 1) {
			WeightedRSocket rsc1 = null;
			WeightedRSocket rsc2 = null;

			int bound = size - 1;
			int i = 0;
			do {
				int i1 = rnd.nextInt(size);
				int i2 = rnd.nextInt(bound);

				if (i2 >= i1) {
					i2++;
				}
				rsc1 = (WeightedRSocket) rSockets.get(i1);
				rsc2 = (WeightedRSocket) rSockets.get(i2);
				if (rsc1.availability() > 0.0 && rsc2.availability() > 0.0) {
					break;
				}
				i++;
			}
			while (i < EFFORT);

			double w1 = algorithmicWeight(rsc1, expFactor, lowerQuantile, higherQuantile);
			double w2 = algorithmicWeight(rsc2, expFactor, lowerQuantile, higherQuantile);
			if (w1 < w2) {
				rSocket = rsc2;
			}
			else {
				rSocket = rsc1;
			}

		}
		else {
			return null;
		}

		return Mono.just(new Response(rSocket));
	}

	private static double algorithmicWeight(
			WeightedRSocket socket, double expFactor, Quantile lowerQuantile, Quantile higherQuantile) {
		if (socket == null || socket.availability() == 0.0) {
			return 0.0;
		}
		int pendings = socket.pending();
		double latency = socket.getPredictedLatency();

		double low = lowerQuantile.estimation();
		double high =
				Math.max(
						higherQuantile.estimation(),
						low * 1.001); // ensure higherQuantile > lowerQuantile + .1%
		double bandWidth = Math.max(high - low, 1);

		if (latency < low) {
			double alpha = (low - latency) / bandWidth;
			double bonusFactor = Math.pow(1 + alpha, expFactor);
			latency /= bonusFactor;
		}
		else if (latency > high) {
			double alpha = (latency - high) / bandWidth;
			double penaltyFactor = Math.pow(1 + alpha, expFactor);
			latency *= penaltyFactor;
		}

		return socket.availability() * 1.0 / (1.0 + latency * (pendings + 1));
	}


	public static class Factory implements LoadBalancer.Factory {

		private final WeightedLoadBalancer weightedLoadBalancer;

		public Factory() {
			this(new WeightedLoadBalancer());
		}

		public Factory(WeightedLoadBalancer weightedLoadBalancer) {
			this.weightedLoadBalancer = weightedLoadBalancer;
		}

		@Override
		public LoadBalancer getInstance(Tags tags) {
			return weightedLoadBalancer;
		}
	}
}

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

import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.routing.broker.stats.ExponentialWeightedMovingAverage;
import io.rsocket.routing.broker.stats.FrugalQuantile;
import io.rsocket.routing.broker.stats.Median;
import io.rsocket.routing.broker.stats.Quantile;
import io.rsocket.util.Clock;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Wrapper of a RSocket, it computes statistics about the req/resp calls and update availability
 * accordingly.
 */
public class WeightedRSocket extends RSocketProxy {

	public static final double DEFAULT_LOWER_QUANTILE = 0.5;
	public static final double DEFAULT_HIGHER_QUANTILE = 0.8;

	private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;
	private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
			Clock.unit().convert(1L, TimeUnit.SECONDS);
	private static final int DEFAULT_INTER_ARRIVAL_FACTOR = 500;
	private static final double EPSILON = 1e-4;
	private final Quantile lowerQuantile;
	private final Quantile higherQuantile;
	private final long inactivityFactor;
	private final long tau;
	private final ExponentialWeightedMovingAverage errorPercentage;
	private volatile int pending; // instantaneous rate
	private long stamp; // last timestamp we sent a request
	private long stamp0; // last timestamp we sent a request or receive a response
	private long duration; // instantaneous cumulative duration
	private Median median;
	private ExponentialWeightedMovingAverage interArrivalTime;
	private AtomicLong pendingStreams; // number of active streams

	private volatile int availability = 1;

	public WeightedRSocket(
			RSocket child,
			Quantile lowerQuantile,
			Quantile higherQuantile,
			int inactivityFactor) {
		super(child);
		this.lowerQuantile = lowerQuantile;
		this.higherQuantile = higherQuantile;
		this.inactivityFactor = inactivityFactor;
		long now = Clock.now();
		this.stamp = now;
		this.stamp0 = now;
		this.duration = 0L;
		this.pending = 0;
		this.median = new Median();
		this.interArrivalTime = new ExponentialWeightedMovingAverage(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
		this.pendingStreams = new AtomicLong();
		this.errorPercentage = new ExponentialWeightedMovingAverage(5, TimeUnit.SECONDS, 1.0);
		this.tau = Clock.unit().convert((long) (5 / Math.log(2)), TimeUnit.SECONDS);
	}

	//FIXME: this shouldn't be needed
	public WeightedRSocket(RSocket child) {
		this(child, new FrugalQuantile(DEFAULT_LOWER_QUANTILE), new FrugalQuantile(DEFAULT_HIGHER_QUANTILE));
	}


	public WeightedRSocket(
			RSocket child, Quantile lowerQuantile, Quantile higherQuantile) {
		this(child, lowerQuantile, higherQuantile, DEFAULT_INTER_ARRIVAL_FACTOR);
	}

	@Override
	public Mono<Payload> requestResponse(Payload payload) {
		long start = incr();
		try {
			return source
					.requestResponse(payload)
					.doOnCancel(() -> decr(start))
					.doOnSuccessOrError(
							(p, t) -> {
								long now = decr(start);
								if (t == null || t instanceof TimeoutException) {
									observe(now - start);
								}

								if (t != null) {
									updateErrorPercentage(0.0);
								}
								else {
									updateErrorPercentage(1.0);
								}
							});
		}
		catch (Throwable t) {
			decr(start);
			updateErrorPercentage(0.0);
			return Mono.error(t);
		}
	}

	@Override
	public Flux<Payload> requestStream(Payload payload) {
		try {
			incrPendingStreams();
			return source
					.requestStream(payload)
					.doFinally(s -> decrPendingStreams())
					.doOnNext(o -> updateErrorPercentage(1.0))
					.doOnError(t -> updateErrorPercentage(0.0));
		}
		catch (Throwable t) {
			decrPendingStreams();
			updateErrorPercentage(0.0);
			return Flux.error(t);
		}
	}

	@Override
	public Mono<Void> fireAndForget(Payload payload) {
		long start = incr();
		try {
			return source
					.fireAndForget(payload)
					.doOnCancel(() -> decr(start))
					.doOnSuccessOrError(
							(p, t) -> {
								long now = decr(start);
								if (t == null || t instanceof TimeoutException) {
									observe(now - start);
								}

								if (t != null) {
									updateErrorPercentage(0.0);
								}
								else {
									updateErrorPercentage(1.0);
								}
							});
		}
		catch (Throwable t) {
			decr(start);
			updateErrorPercentage(0.0);
			return Mono.error(t);
		}
	}

	@Override
	public Mono<Void> metadataPush(Payload payload) {
		long start = incr();
		try {
			return source
					.metadataPush(payload)
					.doOnCancel(() -> decr(start))
					.doOnSuccessOrError(
							(p, t) -> {
								long now = decr(start);
								if (t == null || t instanceof TimeoutException) {
									observe(now - start);
								}

								if (t != null) {
									updateErrorPercentage(0.0);
								}
								else {
									updateErrorPercentage(1.0);
								}
							});
		}
		catch (Throwable t) {
			decr(start);
			updateErrorPercentage(0.0);
			return Mono.error(t);
		}
	}

	@Override
	public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
		try {
			incrPendingStreams();
			return source
					.requestChannel(payloads)
					.doFinally(s -> decrPendingStreams())
					.doOnNext(o -> updateErrorPercentage(1.0))
					.doOnError(t -> updateErrorPercentage(0.0));
		}
		catch (Throwable t) {
			decrPendingStreams();
			updateErrorPercentage(0.0);
			return Flux.error(t);
		}
	}

	public synchronized double getPredictedLatency() {
		long now = Clock.now();
		long elapsed = Math.max(now - stamp, 1L);

		double weight;
		double prediction = median.estimation();

		if (prediction == 0.0) {
			if (pending == 0) {
				weight = 0.0; // first request
			}
			else {
				// subsequent requests while we don't have any history
				weight = STARTUP_PENALTY + pending;
			}
		}
		else if (pending == 0 && elapsed > inactivityFactor * interArrivalTime.value()) {
			// if we did't see any data for a while, we decay the prediction by inserting
			// artificial 0.0 into the median
			median.insert(0.0);
			weight = median.estimation();
		}
		else {
			double predicted = prediction * pending;
			double instant = instantaneous(now);

			if (predicted < instant) { // NB: (0.0 < 0.0) == false
				weight = instant / pending; // NB: pending never equal 0 here
			}
			else {
				// we are under the predictions
				weight = prediction;
			}
		}

		return weight;
	}

	private synchronized long instantaneous(long now) {
		return duration + (now - stamp0) * pending;
	}

	void incrPendingStreams() {
		pendingStreams.incrementAndGet();
	}

	void decrPendingStreams() {
		pendingStreams.decrementAndGet();
	}

	synchronized long incr() {
		long now = Clock.now();
		interArrivalTime.insert(now - stamp);
		duration += Math.max(0, now - stamp0) * pending;
		pending += 1;
		stamp = now;
		stamp0 = now;
		return now;
	}

	synchronized long decr(long timestamp) {
		long now = Clock.now();
		duration += Math.max(0, now - stamp0) * pending - (now - timestamp);
		pending -= 1;
		stamp0 = now;
		return now;
	}

	synchronized void observe(double rtt) {
		median.insert(rtt);
		lowerQuantile.insert(rtt);
		higherQuantile.insert(rtt);
	}

	@Override
	public double availability() {
		if (Clock.now() - stamp > tau) {
			updateErrorPercentage(1.0);
		}
		return source.availability() * errorPercentage.value();
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", WeightedRSocket.class
				.getSimpleName() + "[", "]")
				.add("median=" + median.estimation())
				.add("lowerQuantile=" + lowerQuantile.estimation())
				.add("higherQuantile=" + higherQuantile.estimation())
				.add("interArrivalTime=" + interArrivalTime.value())
				.add("duration/pending=" + (pending == 0 ? 0 : (double) duration / pending))
				.add("pending=" + pending)
				.add("availability=" + availability())
				//.add("tau=" + tau)
				//.add("errorPercentage=" + errorPercentage)
				//.add("stamp=" + stamp)
				//.add("stamp0=" + stamp0)
				//.add("duration=" + duration)
				//.add("inactivityFactor=" + inactivityFactor)
				//.add("pendingStreams=" + pendingStreams)
				.add("source=" + source)
				.toString();
	}

	synchronized void updateErrorPercentage(double value) {
		errorPercentage.insert(value);
	}

	public int pending() {
		return pending;
	}

	public RSocket getSource() {
		return source;
	}

}

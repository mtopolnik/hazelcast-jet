/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.accumulator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static java.util.Collections.EMPTY_SET;

/**
 * Maintains the components needed to compute the linear regression on a
 * set of {@code (long, long)} pairs and additionally retains all the
 * individual data points to be able to compute the standard deviation
 * from the fitted affine function. The data is retained in {@code
 * long[]} arrays (16 bytes per item) and the finished value is a {@code
 * Map.Entry<Double, Double>} where the key is the fitted linear
 * coefficient and the value is the standard deviation (also called
 * the root-mean-square error or RMSE).
 */
public final class LinTrendWithErrorAcc {
    private final Samples ownSamples = new Samples();
    private final LinTrendAccumulator lrAcc = new LinTrendAccumulator();
    private Set<Samples> combinedSamples = EMPTY_SET;

    public LinTrendWithErrorAcc accumulate(long x, long y) {
        lrAcc.accumulate(x, y);
        ownSamples.accumulate(x, y);
        return this;
    }

    public LinTrendWithErrorAcc combine(LinTrendWithErrorAcc that) {
        lrAcc.combine(that.lrAcc);
        if (combinedSamples == EMPTY_SET) {
            combinedSamples = new HashSet<>();
        }
        combinedSamples.add(that.ownSamples);
        combinedSamples.addAll(that.combinedSamples);
        return this;
    }

    public LinTrendWithErrorAcc deduct(LinTrendWithErrorAcc that) {
        lrAcc.deduct(that.lrAcc);
        combinedSamples.remove(that.ownSamples);
        combinedSamples.removeAll(that.combinedSamples);
        return this;
    }

    public Entry<Double, Double> finish() {
        // fitted function: (y - yBase) - b(x - xBase) - a = 0
        double b = lrAcc.finish();
        combinedSamples.add(ownSamples);
        long xBase = ownSamples.xs[0];
        long yBase = ownSamples.ys[0];
        long count = 0;

        // compute mean residual for a = 0:
        double residualSum = 0;
        for (Samples s : combinedSamples) {
            count += s.xs.length;
            for (int i = 0; i < s.xs.length; i++) {
                long x = s.xs[i];
                long y = s.ys[i];
                residualSum += (y - yBase) - b * (x - xBase);
            }
        }

        // determine the `a` that makes the mean residual vanish:
        double a = residualSum / count;

        // compute the mean square residual:
        double sumSquareResidual = 0;
        for (Samples s : combinedSamples) {
            for (int i = 0; i < s.xs.length; i++) {
                long x = s.xs[i];
                long y = s.ys[i];
                double residual = (y - yBase) - b * (x - xBase) - a;
                sumSquareResidual += residual * residual;
            }
        }
        double meanSquareResidual = sumSquareResidual / count;

        return entry(b, Math.sqrt(meanSquareResidual));
    }

    private static class Samples {
        private long[] xs = new long[2];
        private long[] ys = new long[2];

        private int count;

        void accumulate(long x, long y) {
            ensureCapacity();
            xs[count] = x;
            ys[count] = y;
            count++;
        }

        private void ensureCapacity() {
            if (count == xs.length) {
                xs = Arrays.copyOf(xs, xs.length * 3 / 2);
                ys = Arrays.copyOf(ys, ys.length * 3 / 2);
            }
        }
    }
}

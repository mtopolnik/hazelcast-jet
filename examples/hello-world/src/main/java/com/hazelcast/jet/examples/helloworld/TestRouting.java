/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.helloworld;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class TestRouting {
    public static void main(String[] args) {
        System.out.println(MockSource.ITEM.length());
        DAG dag = new DAG();
        StreamSourceTransform<String> source = (StreamSourceTransform<String>)
                SourceBuilder.stream("mock-source", MockSource::new)
                             .fillBufferFn(MockSource::fillBuffer)
                             .build();
        Vertex srcVertex = dag.newVertex("source", source.metaSupplierFn.apply(noEventTime()));
//        Vertex mapVertex = dag.newVertex("map", Processors.mapP(x -> x));
        Vertex sinkVertex = dag.newVertex("sink", SinkProcessor::new).localParallelism(5);
//        dag.edge(between(srcVertex, mapVertex));
        dag.edge(between(srcVertex, sinkVertex).distributed());

        JetConfig cfg = new JetConfig();
//        cfg.getDefaultEdgeConfig().setQueueSize(2);

        JetInstance jet1 = Jet.newJetInstance(cfg);
        JetInstance jet2 = Jet.newJetInstance(cfg);
        JetInstance jet3 = Jet.newJetInstance(cfg);
        jet1.newJob(dag);
    }
}

class MockSource {
    static final long SEND_PERIOD_NANOS = MICROSECONDS.toNanos(100);

    static final String ITEM = DoubleStream.iterate(1, d -> d * 1.00000001d)
                                           .limit(10).boxed()
                                           .map(Object::toString)
                                           .collect(Collectors.joining(", "));

    long nextSendSchedule = System.nanoTime();

    MockSource(Processor.Context ctx) {
    }

    void fillBuffer(SourceBuffer<String> buf) {
        long now = System.nanoTime(); // nextSendSchedule + 128 * SEND_PERIOD_NANOS;
        for (; nextSendSchedule <= now; nextSendSchedule += SEND_PERIOD_NANOS) {
            buf.add(ITEM);
        }
    }
}

class SinkProcessor implements Processor {
    static final long REPORT_PERIOD = TimeUnit.MILLISECONDS.toNanos(1000);

    int processorIndex;
    int totalParallelism;

    long count;
    long lastReported;

    @Override
    public void init(@Nonnull Outbox outbox, Context context) {
        processorIndex = context.globalProcessorIndex();
        totalParallelism = context.totalParallelism();
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        count += inbox.size();
        int processorFactor = 1; // processorIndex == 0 ? 1 : 2;
        int commonFactor = 10;
        inbox.drain(o -> {
            double ret = 0;
            String item = (String) o;
            for (int i = 0; i < processorFactor * item.length(); i++) {
                ret += Math.pow(i, i / (double) commonFactor);
            }
            if (ret < 0) { // Never happens, but ensures the result is used non-trivially
                System.out.println("ret < 0");
            }
            long now = System.nanoTime();
            if (now - lastReported <= REPORT_PERIOD) {
                return;
            }
            System.out.format("%s %2d saw %,10d items%n",
                    (TimeUnit.NANOSECONDS.toMillis(now) / 500) % 4 == 0 ? "" : "    ",
                    processorIndex, count);
            lastReported = (now / REPORT_PERIOD) * REPORT_PERIOD;
            count = 0;
        });
    }
}

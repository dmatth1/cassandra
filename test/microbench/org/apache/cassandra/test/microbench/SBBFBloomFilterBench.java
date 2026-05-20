/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.SBBFBloomFilter;

/**
 * Per-probe latency for {@link SBBFBloomFilter} vs the classical
 * {@link org.apache.cassandra.utils.BloomFilter}, both sized for 1% target FP. Tiers
 * are picked so the filter footprint lands in L2 (S), L3 (M), around-L3 (L), or DRAM
 * (XL); production SSTable bloom on large tables is closest to XL hit.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Benchmark)
public class SBBFBloomFilterBench
{
    @Param({"S", "M", "L", "XL"})
    public String tier;

    @Param({"miss", "hit"})
    public String workload;

    @Param({"sbbf", "classical"})
    public String impl;

    private IFilter filter;
    private IFilter.FilterKey[] probeKeys;
    private int next;

    @Setup(Level.Trial)
    public void setup()
    {
        long n;
        switch (tier)
        {
            case "S":  n =     16_384L; break;
            case "M":  n =    262_144L; break;
            case "L":  n =  4_194_304L; break;
            case "XL": n = 67_108_864L; break;
            default: throw new IllegalArgumentException("unknown tier: " + tier);
        }
        filter = "sbbf".equals(impl) ? SBBFBloomFilter.withEstimates(n, 0.01)
                                     : FilterFactory.getFilter(n, 0.01);

        int inserted = (int) Math.min(n, 1_048_576);
        IFilter.FilterKey[] insertedKeys = new IFilter.FilterKey[inserted];
        Random r = new Random(0xCAFEBABEL);
        for (int i = 0; i < inserted; i++)
        {
            byte[] k = new byte[16];
            r.nextBytes(k);
            insertedKeys[i] = wrap(k);
            filter.add(insertedKeys[i]);
        }

        if ("hit".equals(workload))
        {
            probeKeys = insertedKeys;
        }
        else
        {
            probeKeys = new IFilter.FilterKey[1 << 14];
            Random r2 = new Random(0xDEADBEEFL);
            for (int i = 0; i < probeKeys.length; i++)
            {
                byte[] k = new byte[16];
                r2.nextBytes(k);
                probeKeys[i] = wrap(k);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
        filter.close();
    }

    @Benchmark
    public void probe(Blackhole bh)
    {
        bh.consume(filter.isPresent(probeKeys[(next = (next + 1) & (probeKeys.length - 1))]));
    }

    private static IFilter.FilterKey wrap(byte[] k)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(0L), ByteBuffer.wrap(k));
    }
}

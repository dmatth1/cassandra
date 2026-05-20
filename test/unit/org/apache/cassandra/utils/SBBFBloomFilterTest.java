/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class SBBFBloomFilterTest
{
    @Test
    public void testAddPresent()
    {
        try (SBBFBloomFilter f = SBBFBloomFilter.withEstimates(1024, 0.01))
        {
            f.add(FilterTestHelper.bytes("hello"));
            Assert.assertTrue(f.isPresent(FilterTestHelper.bytes("hello")));
            Assert.assertFalse(f.isPresent(FilterTestHelper.bytes("goodbye")));
        }
    }

    @Test
    public void testNoFalseNegatives()
    {
        try (SBBFBloomFilter f = SBBFBloomFilter.withEstimates(10_000, 0.01))
        {
            for (int i = 0; i < 10_000; i++)
                f.add(FilterTestHelper.bytes("key-" + i));
            for (int i = 0; i < 10_000; i++)
                Assert.assertTrue("missing key-" + i, f.isPresent(FilterTestHelper.bytes("key-" + i)));
        }
    }

    @Test
    public void testFalsePositiveRateWithinTarget()
    {
        // SBBF at K=8 sized for 1% FP using the classical bits/element formula
        // actually gives a meaningfully lower observed FP rate; we just check it
        // stays well under 2x the target.
        int n = 50_000;
        double fp = 0.01;
        try (SBBFBloomFilter f = SBBFBloomFilter.withEstimates(n, fp))
        {
            Random r = new Random(0xCAFEBABEL);
            byte[] buf = new byte[16];
            for (int i = 0; i < n; i++)
            {
                r.nextBytes(buf);
                f.add(new ByteArrayKey(buf.clone()));
            }
            int fps = 0;
            int trials = 100_000;
            r = new Random(0xDEADBEEFL);
            for (int i = 0; i < trials; i++)
            {
                r.nextBytes(buf);
                if (f.isPresent(new ByteArrayKey(buf.clone())))
                    fps++;
            }
            double observed = (double) fps / trials;
            Assert.assertTrue("observed FP " + observed + " exceeds 2x target " + (2 * fp), observed < 2 * fp);
        }
    }

    @Test
    public void testVariableKeyLengths()
    {
        // hashVar path must produce no false negatives for every (length mod 8) tail case.
        try (SBBFBloomFilter f = SBBFBloomFilter.withEstimates(8192, 0.01))
        {
            for (int len = 0; len <= 128; len++)
            {
                byte[] k = new byte[len];
                for (int i = 0; i < len; i++) k[i] = (byte) (i + 1);
                ByteArrayKey key = new ByteArrayKey(k);
                f.add(key);
                Assert.assertTrue("len " + len + " not present after add", f.isPresent(key));
            }
        }
    }

    @Test
    public void testNonDecoratedFilterKeyFallback()
    {
        // FilterKey impls that aren't DecoratedKey take the fallback path through
        // filterHash64()'s default impl, which folds filterHash(long[]).
        try (SBBFBloomFilter f = SBBFBloomFilter.withEstimates(1024, 0.01))
        {
            ByteArrayKey present = new ByteArrayKey("present".getBytes());
            f.add(present);
            Assert.assertTrue(f.isPresent(present));
            Assert.assertFalse(f.isPresent(new ByteArrayKey("absent".getBytes())));
        }
    }

    @Test
    public void testClear()
    {
        try (SBBFBloomFilter f = SBBFBloomFilter.withEstimates(1024, 0.01))
        {
            f.add(FilterTestHelper.bytes("x"));
            Assert.assertTrue(f.isPresent(FilterTestHelper.bytes("x")));
            f.clear();
            Assert.assertFalse(f.isPresent(FilterTestHelper.bytes("x")));
        }
    }

    @Test
    public void testSerializeRoundTrip() throws IOException
    {
        try (SBBFBloomFilter f = SBBFBloomFilter.withEstimates(10_000, 0.01))
        {
            for (int i = 0; i < 1000; i++)
                f.add(FilterTestHelper.bytes("key-" + i));

            DataOutputBuffer out = new DataOutputBuffer();
            BloomFilterSerializer.forVersion(false).serialize(f, out);
            Assert.assertEquals(f.serializedSize(false), out.getLength());

            ByteArrayInputStream bin = new ByteArrayInputStream(out.getData(), 0, out.getLength());
            try (IFilter f2 = BloomFilterSerializer.forVersion(false).deserialize(Util.DataInputStreamPlusImpl.wrap(bin)))
            {
                Assert.assertTrue(f2 instanceof SBBFBloomFilter);
                Assert.assertEquals(f.numBlocks(), ((SBBFBloomFilter) f2).numBlocks());
                for (int i = 0; i < 1000; i++)
                    Assert.assertTrue("missing after deserialize: key-" + i,
                                      f2.isPresent(FilterTestHelper.bytes("key-" + i)));
            }
        }
    }

    @Test
    public void testClassicalStillDeserializesOnSharedSerializer() throws IOException
    {
        // Existing classical-format SSTables must continue to round-trip; SBBF only
        // activates when the magic header is present.
        try (IFilter classical = FilterFactory.getFilter(10_000L, 0.01))
        {
            classical.add(FilterTestHelper.bytes("a"));
            DataOutputBuffer out = new DataOutputBuffer();
            BloomFilterSerializer.forVersion(false).serialize(classical, out);
            ByteArrayInputStream bin = new ByteArrayInputStream(out.getData(), 0, out.getLength());
            try (IFilter back = BloomFilterSerializer.forVersion(false).deserialize(Util.DataInputStreamPlusImpl.wrap(bin)))
            {
                Assert.assertTrue(back instanceof BloomFilter);
                Assert.assertTrue(back.isPresent(FilterTestHelper.bytes("a")));
            }
        }
    }

    @Test
    public void testSharedCopyClosesIndependently()
    {
        SBBFBloomFilter f = SBBFBloomFilter.withEstimates(1024, 0.01);
        f.add(FilterTestHelper.bytes("hello"));
        SBBFBloomFilter copy = f.sharedCopy();
        Assert.assertTrue(copy.isPresent(FilterTestHelper.bytes("hello")));
        f.close();
        Assert.assertTrue(copy.isPresent(FilterTestHelper.bytes("hello")));
        copy.close();
    }

    @Test
    public void testWithEstimatesArgumentValidation()
    {
        assertRejects(() -> SBBFBloomFilter.withEstimates(0, 0.01));
        assertRejects(() -> SBBFBloomFilter.withEstimates(-1, 0.01));
        assertRejects(() -> SBBFBloomFilter.withEstimates(1000, 0.0));
        assertRejects(() -> SBBFBloomFilter.withEstimates(1000, 1.0));
        assertRejects(() -> SBBFBloomFilter.withEstimates(1000, -0.5));
    }

    @Test(expected = AssertionError.class)
    public void testSerializeRejectsOldFormat() throws IOException
    {
        try (SBBFBloomFilter f = SBBFBloomFilter.withEstimates(1024, 0.01))
        {
            f.serialize(new DataOutputBuffer(), true);
        }
    }

    private static void assertRejects(Runnable r)
    {
        try { r.run(); Assert.fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { }
    }

    private static final class ByteArrayKey implements IFilter.FilterKey
    {
        private final byte[] data;
        ByteArrayKey(byte[] data) { this.data = data; }
        @Override
        public void filterHash(long[] dest)
        {
            MurmurHash.hash3_x64_128(ByteBuffer.wrap(data), 0, data.length, 0, dest);
        }
    }
}

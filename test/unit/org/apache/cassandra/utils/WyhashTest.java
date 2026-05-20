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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class WyhashTest
{
    @Test
    public void testByteArrayMatchesByteBufferEveryLength()
    {
        // The two entry points are independent code paths (byte[] vs ByteBuffer
        // reads) but must produce bit-identical output for the same key bytes,
        // covering every (length mod 8) tail case and the hash16 fast path.
        Random r = new Random(0xCAFEBABEL);
        for (int len = 0; len <= 73; len++)
        {
            byte[] buf = new byte[len];
            r.nextBytes(buf);
            long fromArray  = Wyhash.hash(buf, 0, len, Wyhash.SEED);
            long fromBuffer = Wyhash.hash(ByteBuffer.wrap(buf), 0, len, Wyhash.SEED);
            Assert.assertEquals("len " + len, fromArray, fromBuffer);
        }
    }

    @Test
    public void testOffsetVariantsAgreeWithFullArray()
    {
        // hash(byte[], offset, length, seed) must produce the same result as
        // hash(slice, 0, length, seed) for arbitrary offsets.
        Random r = new Random(0x0FF1CEL);
        for (int len = 0; len <= 40; len++)
        {
            int prefix = 7;
            byte[] padded = new byte[prefix + len + 11];
            r.nextBytes(padded);
            byte[] slice = new byte[len];
            System.arraycopy(padded, prefix, slice, 0, len);
            Assert.assertEquals("len " + len,
                                Wyhash.hash(slice,  0,      len, Wyhash.SEED),
                                Wyhash.hash(padded, prefix, len, Wyhash.SEED));
        }
    }

    @Test
    public void testHashSensitiveToEveryByte()
    {
        // Flipping any single bit must change the hash.
        byte[] base = new byte[16];
        new Random(0x5BBFL).nextBytes(base);
        long baseHash = Wyhash.hash(base, 0, 16, Wyhash.SEED);
        for (int i = 0; i < 16; i++)
            for (int bit = 0; bit < 8; bit++)
            {
                byte[] perturbed = base.clone();
                perturbed[i] ^= (byte) (1 << bit);
                Assert.assertNotEquals("byte " + i + " bit " + bit,
                                       baseHash, Wyhash.hash(perturbed, 0, 16, Wyhash.SEED));
            }
    }

    @Test
    public void testDifferentSeedsDiffer()
    {
        byte[] buf = "the quick brown fox".getBytes();
        Assert.assertNotEquals(Wyhash.hash(buf, 0, buf.length, 0L),
                               Wyhash.hash(buf, 0, buf.length, 1L));
    }

    @Test
    public void testDistribution()
    {
        // 10K random 16-byte keys must produce 10K distinct hashes (collision
        // probability with a good 64-bit hash is < 1 in 2^44).
        Random r = new Random(0x12345L);
        Set<Long> seen = new HashSet<>();
        byte[] buf = new byte[16];
        for (int i = 0; i < 10_000; i++)
        {
            r.nextBytes(buf);
            seen.add(Wyhash.hash(buf, 0, 16, Wyhash.SEED));
        }
        Assert.assertEquals("hash collisions on 10K random 16-byte keys", 10_000, seen.size());
    }
}

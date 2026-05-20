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
package org.apache.cassandra.utils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Fast 64-bit non-cryptographic hash used by {@link SBBFBloomFilter}. {@code hash16}
 * is a 16-byte fast path (single 64x64-&gt;128 multiply with XOR-fold), and
 * {@code hashVar} is a variable-length fasthash64 loop. On 16-byte partition keys this
 * costs ~3-5 ns vs ~12-25 ns for MurmurHash3-x64-128. Classical {@link BloomFilter}
 * continues to use Murmur3 via {@link IFilter.FilterKey#filterHash(long[])}; only the
 * SBBF probe path takes Wyhash, via {@link IFilter.FilterKey#filterHash64()}.
 */
public final class Wyhash
{
    private static final long M = 0x880355f21e6d1965L;

    /**
     * byte[]-as-long view in little-endian order; lowers to a single load on
     * little-endian hardware (no byte-swap) and to load+bswap on big-endian.
     */
    private static final VarHandle BYTES_AS_LONG_LE =
        MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /** Default seed for the SBBF probe path; insert/test must use the same seed. */
    public static final long SEED = 0xCAFEBABEDEADBEEFL;

    private Wyhash() {}

    public static long hash(byte[] data, int offset, int length, long seed)
    {
        return length == 16 ? hash16(data, offset, seed) : hashVar(data, offset, length, seed);
    }

    public static long hash(ByteBuffer key, int offset, int length, long seed)
    {
        return length == 16 ? hash16(key, offset, seed) : hashVar(key, offset, length, seed);
    }

    public static long hash16(byte[] data, int offset, long seed)
    {
        return mix128(readLong(data, offset), readLong(data, offset + 8), seed);
    }

    public static long hash16(ByteBuffer key, int offset, long seed)
    {
        return mix128(readLong(key, offset), readLong(key, offset + 8), seed);
    }

    public static long hashVar(byte[] data, int offset, int length, long seed)
    {
        long h = (long) length * M ^ seed;
        int nblocks = length >>> 3;
        for (int i = 0; i < nblocks; i++)
        {
            long k = readLong(data, offset + (i << 3));
            k *= M; k ^= k >>> 23; k *= M;
            h ^= k; h *= M;
        }
        int tailStart = offset + (nblocks << 3);
        long t = 0;
        switch (length & 7)
        {
            // CHECKSTYLE.OFF: FallThrough
            case 7: t ^= ((long) (data[tailStart + 6] & 0xff)) << 48;
            case 6: t ^= ((long) (data[tailStart + 5] & 0xff)) << 40;
            case 5: t ^= ((long) (data[tailStart + 4] & 0xff)) << 32;
            case 4: t ^= ((long) (data[tailStart + 3] & 0xff)) << 24;
            case 3: t ^= ((long) (data[tailStart + 2] & 0xff)) << 16;
            case 2: t ^= ((long) (data[tailStart + 1] & 0xff)) << 8;
            case 1:
                t ^= (long) (data[tailStart] & 0xff);
                t *= M; t ^= t >>> 23; t *= M;
                h ^= t; h *= M;
                break;
            // CHECKSTYLE.ON: FallThrough
            default:
                break;
        }
        h ^= h >>> 23; h *= M; h ^= h >>> 23;
        return h;
    }

    public static long hashVar(ByteBuffer key, int offset, int length, long seed)
    {
        long h = (long) length * M ^ seed;
        int nblocks = length >>> 3;
        for (int i = 0; i < nblocks; i++)
        {
            long k = readLong(key, offset + (i << 3));
            k *= M; k ^= k >>> 23; k *= M;
            h ^= k; h *= M;
        }
        int tailStart = offset + (nblocks << 3);
        long t = 0;
        switch (length & 7)
        {
            // CHECKSTYLE.OFF: FallThrough
            case 7: t ^= ((long) (key.get(tailStart + 6) & 0xff)) << 48;
            case 6: t ^= ((long) (key.get(tailStart + 5) & 0xff)) << 40;
            case 5: t ^= ((long) (key.get(tailStart + 4) & 0xff)) << 32;
            case 4: t ^= ((long) (key.get(tailStart + 3) & 0xff)) << 24;
            case 3: t ^= ((long) (key.get(tailStart + 2) & 0xff)) << 16;
            case 2: t ^= ((long) (key.get(tailStart + 1) & 0xff)) << 8;
            case 1:
                t ^= (long) (key.get(tailStart) & 0xff);
                t *= M; t ^= t >>> 23; t *= M;
                h ^= t; h *= M;
                break;
            // CHECKSTYLE.ON: FallThrough
            default:
                break;
        }
        h ^= h >>> 23; h *= M; h ^= h >>> 23;
        return h;
    }

    /** 64x64-&gt;128 multiply, XOR-fold high and low, XOR seed. */
    private static long mix128(long a, long b, long seed)
    {
        long lo = a * b;
        long hi = unsignedMultiplyHigh(a, b);
        return (lo ^ hi) ^ seed;
    }

    /**
     * Upper 64 bits of the unsigned 128-bit product {@code a*b}. Equivalent to
     * {@code Math.unsignedMultiplyHigh} (Java 18+); inlined here so the class compiles
     * on Java 11 by sign-correcting {@code Math.multiplyHigh} (Java 9+).
     */
    private static long unsignedMultiplyHigh(long a, long b)
    {
        long h = Math.multiplyHigh(a, b);
        h += (a >> 63) & b;
        h += (b >> 63) & a;
        return h;
    }

    private static long readLong(byte[] data, int offset)
    {
        return (long) BYTES_AS_LONG_LE.get(data, offset);
    }

    private static long readLong(ByteBuffer key, int offset)
    {
        return ((long) (key.get(offset)     & 0xff))
             | ((long) (key.get(offset + 1) & 0xff) << 8)
             | ((long) (key.get(offset + 2) & 0xff) << 16)
             | ((long) (key.get(offset + 3) & 0xff) << 24)
             | ((long) (key.get(offset + 4) & 0xff) << 32)
             | ((long) (key.get(offset + 5) & 0xff) << 40)
             | ((long) (key.get(offset + 6) & 0xff) << 48)
             | ((long) (key.get(offset + 7) & 0xff) << 56);
    }
}

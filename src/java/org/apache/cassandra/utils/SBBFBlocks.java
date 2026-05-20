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

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * Off-heap byte storage for {@link SBBFBloomFilter}. Each block is a contiguous
 * 32-byte region (eight 32-bit lanes, little-endian). Mirrors the {@code OffHeapBitSet}
 * / {@link Memory} ownership pattern used by classical {@link BloomFilter}.
 */
public final class SBBFBlocks implements AutoCloseable
{
    private final Memory bytes;

    private SBBFBlocks(Memory bytes)
    {
        this.bytes = bytes;
    }

    public static SBBFBlocks allocate(long numBlocks)
    {
        long byteCount = numBlocks * SBBFBloomFilter.BLOCK_BYTES;
        if (byteCount <= 0)
            throw new IllegalArgumentException("Invalid SBBF byte count: " + byteCount);
        Memory mem;
        try
        {
            mem = Memory.allocate(byteCount);
        }
        catch (OutOfMemoryError e)
        {
            throw new RuntimeException("Out of native memory while allocating SBBF blocks. "
                                       + "Reduce bloom_filter_fp_chance or add system RAM.");
        }
        mem.setMemory(0, byteCount, (byte) 0);
        return new SBBFBlocks(mem);
    }

    public long offHeapSize()
    {
        return bytes.size();
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        identities.add(bytes);
    }

    /** Read the little-endian int lane at the given byte offset. */
    public int getLane(long byteOffset)
    {
        return bytes.getInt(byteOffset);
    }

    /** OR {@code mask} into the int lane at the given byte offset. */
    public void orLane(long byteOffset, int mask)
    {
        bytes.setInt(byteOffset, bytes.getInt(byteOffset) | mask);
    }

    public void clear()
    {
        bytes.setMemory(0, bytes.size(), (byte) 0);
    }

    public void write(DataOutputStreamPlus out) throws IOException
    {
        out.write(bytes, 0, bytes.size());
    }

    /**
     * Raw byte-copy from {@code in}; serialize wrote the off-heap region verbatim
     * (little-endian), so {@code readLong} would silently byte-swap each int lane.
     */
    public void readFrom(DataInputStreamPlus in) throws IOException
    {
        FBUtilities.copy(in, new MemoryOutputStream(bytes), bytes.size());
    }

    @Override
    public void close()
    {
        bytes.free();
    }
}

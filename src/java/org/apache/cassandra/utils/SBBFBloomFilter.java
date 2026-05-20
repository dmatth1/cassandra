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
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;

import net.nicoulaj.compilecommand.annotations.Inline;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.WrappedSharedCloseable;

/**
 * Split Block Bloom Filter (Apache Parquet layout) implementation of {@link IFilter}.
 *
 * Clusters all K bits for a single key into one 256-bit block, so each probe touches
 * exactly one cache line. K=8 lanes are packed on 32-bit boundaries with the Parquet
 * SALT constants; lane {@code i}'s bit is {@code (key * SALT[i]) >>> 27}. Reference:
 * Putze, Sanders, Singler, "Cache-, Hash- and Space-Efficient Bloom Filters" (JEA 2007).
 *
 * The probe hash is Wyhash on the SBBF side ({@link IFilter.FilterKey#filterHash64()}),
 * not the Murmur3 used by classical {@link BloomFilter}.
 */
public class SBBFBloomFilter extends WrappedSharedCloseable implements IFilter
{
    /** Parquet SBBF SALT constants, verbatim from the spec. */
    static final int[] SALT = {
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
        0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
    };

    static final int K = 8;
    static final int BLOCK_BYTES = 32;
    static final int LANE_BYTES = 4;

    public final SBBFBlocks blocks;
    final long numBlocks;
    final long blockMask; // numBlocks - 1; numBlocks must be a power of two

    SBBFBloomFilter(long numBlocks, SBBFBlocks blocks)
    {
        super(blocks);
        if (numBlocks <= 0 || (numBlocks & (numBlocks - 1)) != 0)
            throw new IllegalArgumentException("numBlocks must be a positive power of two; got " + numBlocks);
        this.blocks = blocks;
        this.numBlocks = numBlocks;
        this.blockMask = numBlocks - 1;
    }

    private SBBFBloomFilter(SBBFBloomFilter copy)
    {
        super(copy);
        this.blocks = copy.blocks;
        this.numBlocks = copy.numBlocks;
        this.blockMask = copy.blockMask;
    }

    public long numBlocks()
    {
        return numBlocks;
    }

    @Override
    public void add(FilterKey key)
    {
        addHash(hashOf(key));
    }

    @Override
    public boolean isPresent(FilterKey key)
    {
        return testHash(hashOf(key));
    }

    /**
     * Resolve a 64-bit Wyhash for {@code key}. The {@code DecoratedKey} fast path
     * skips the {@code filterHash64} virtual dispatch (the SSTable probe sees only
     * DecoratedKey in practice) and routes heap-backed buffers through the byte[]
     * Wyhash path to avoid per-byte ByteBuffer.get dispatch.
     */
    @Inline
    private static long hashOf(FilterKey key)
    {
        if (key instanceof DecoratedKey)
        {
            ByteBuffer buf = ((DecoratedKey) key).getKey();
            if (buf.hasArray())
                return Wyhash.hash(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining(), Wyhash.SEED);
            return Wyhash.hash(buf, buf.position(), buf.remaining(), Wyhash.SEED);
        }
        return key.filterHash64();
    }

    /** Direct prehash insert; bench entry point. */
    @Inline
    public void addHash(long hash)
    {
        long blockIdx = (hash >>> 32) & blockMask;
        int key = (int) hash;
        long base = blockIdx * BLOCK_BYTES;
        for (int i = 0; i < K; i++)
        {
            int bit = (key * SALT[i]) >>> 27;
            blocks.orLane(base + i * LANE_BYTES, 1 << bit);
        }
    }

    /** Direct prehash test; branching early-exit on the first unset lane. */
    @Inline
    public boolean testHash(long hash)
    {
        long blockIdx = (hash >>> 32) & blockMask;
        int key = (int) hash;
        long base = blockIdx * BLOCK_BYTES;
        for (int i = 0; i < K; i++)
        {
            int bit = (key * SALT[i]) >>> 27;
            if (((blocks.getLane(base + i * LANE_BYTES) >>> bit) & 1) == 0)
                return false;
        }
        return true;
    }

    @Override
    public void clear()
    {
        blocks.clear();
    }

    @Override
    public long serializedSize(boolean oldSerializationFormat)
    {
        assert !oldSerializationFormat : "SBBF cannot be serialised in the legacy classical-bloom format";
        return TypeSizes.sizeof(BloomFilterSerializer.SBBF_MAGIC)
               + TypeSizes.sizeof(numBlocks)
               + numBlocks * BLOCK_BYTES;
    }

    @Override
    public void serialize(DataOutputStreamPlus out, boolean oldSerializationFormat) throws IOException
    {
        assert !oldSerializationFormat : "SBBF cannot be serialised in the legacy classical-bloom format";
        out.writeInt(BloomFilterSerializer.SBBF_MAGIC);
        out.writeLong(numBlocks);
        blocks.write(out);
    }

    /** Deserialise after the magic int has been consumed by {@link BloomFilterSerializer}. */
    static SBBFBloomFilter deserializeAfterMagic(DataInputStreamPlus in) throws IOException
    {
        long numBlocks = in.readLong();
        if (numBlocks <= 0 || (numBlocks & (numBlocks - 1)) != 0)
            throw new IOException("Invalid SBBF numBlocks (not a positive power of two): " + numBlocks);
        SBBFBlocks blocks = SBBFBlocks.allocate(numBlocks);
        try
        {
            blocks.readFrom(in);
        }
        catch (IOException | RuntimeException e)
        {
            blocks.close();
            throw e;
        }
        return new SBBFBloomFilter(numBlocks, blocks);
    }

    @Override
    public SBBFBloomFilter sharedCopy()
    {
        return new SBBFBloomFilter(this);
    }

    @Override
    public long offHeapSize()
    {
        return blocks.offHeapSize();
    }

    @Override
    public boolean isInformative()
    {
        return blocks.offHeapSize() > 0;
    }

    @Override
    public String toString()
    {
        return "SBBFBloomFilter[K=" + K + ";numBlocks=" + numBlocks + ']';
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        blocks.addTo(identities);
    }

    /**
     * Size an SBBF for {@code numElements} at target FP rate {@code fpChance}. Uses the
     * classical Bloom bits/element formula, then rounds the block count to a power of two.
     * SBBF K=8 yields a lower observed FP rate than the requested target at the same
     * bits/element budget.
     */
    public static SBBFBloomFilter withEstimates(long numElements, double fpChance)
    {
        if (numElements <= 0)
            throw new IllegalArgumentException("numElements must be positive; got " + numElements);
        if (fpChance <= 0 || fpChance >= 1)
            throw new IllegalArgumentException("fpChance must be in (0, 1); got " + fpChance);

        double bits = -((double) numElements * Math.log(fpChance)) / (Math.log(2) * Math.log(2));
        long numBlocks = nextPow2(Math.max(1L, (long) Math.ceil(bits / 256.0)));
        if (numBlocks * BLOCK_BYTES < 0)
            throw new UnsupportedOperationException("SBBF size is > 16GB, reduce the bloom_filter_fp_chance");
        return new SBBFBloomFilter(numBlocks, SBBFBlocks.allocate(numBlocks));
    }

    @VisibleForTesting
    static long nextPow2(long x)
    {
        if (x <= 1) return 1;
        x--;
        x |= x >>> 1;  x |= x >>> 2;  x |= x >>> 4;
        x |= x >>> 8;  x |= x >>> 16; x |= x >>> 32;
        return x + 1;
    }
}

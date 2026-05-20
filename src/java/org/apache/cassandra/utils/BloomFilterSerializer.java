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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IGenericSerializer;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;

public final class BloomFilterSerializer implements IGenericSerializer<IFilter, DataInputStreamPlus, DataOutputStreamPlus>
{
    /**
     * Magic int marking the start of a {@link SBBFBloomFilter} on disk.
     * Classical {@link BloomFilter} writes its <code>hashCount</code>
     * (always a small positive int) as the first int. Any negative int
     * in that slot is therefore unambiguously the SBBF dispatch marker;
     * existing on-disk files never collide with it.
     */
    static final int SBBF_MAGIC = 0xCA555BBF;

    public final static BloomFilterSerializer newFormatInstance = new BloomFilterSerializer(false);
    public final static BloomFilterSerializer oldFormatInstance = new BloomFilterSerializer(true);

    private final boolean oldFormat;

    private BloomFilterSerializer(boolean oldFormat)
    {
        this.oldFormat = oldFormat;
    }

    public static BloomFilterSerializer forVersion(boolean oldSerializationFormat)
    {
        if (oldSerializationFormat)
            return oldFormatInstance;

        return newFormatInstance;
    }

    @Override
    public void serialize(IFilter f, DataOutputStreamPlus out) throws IOException
    {
        assert !oldFormat : "Filter should not be serialized in old format";
        if (f instanceof SBBFBloomFilter)
        {
            f.serialize(out, false);
            return;
        }
        if (!(f instanceof BloomFilter))
            throw new IOException("Unknown filter type: " + (f == null ? "null" : f.getClass().getName()));
        BloomFilter bf = (BloomFilter) f;
        out.writeInt(bf.hashCount);
        bf.bitset.serialize(out);
    }

    @Override
    public long serializedSize(IFilter f)
    {
        if (f instanceof SBBFBloomFilter)
            return f.serializedSize(false);
        BloomFilter bf = (BloomFilter) f;
        int size = TypeSizes.sizeof(bf.hashCount);
        size += bf.bitset.serializedSize();
        return size;
    }

    @Override
    public IFilter deserialize(DataInputStreamPlus in) throws IOException
    {
        int first = in.readInt();
        if (first == SBBF_MAGIC)
        {
            if (oldFormat)
                throw new IOException("SBBF filter present on the legacy (oldBf) format path, which cannot carry it");
            return SBBFBloomFilter.deserializeAfterMagic(in);
        }
        // Classical path: `first` is the hashCount; the bitset follows.
        int hashes = first;
        IBitSet bs = OffHeapBitSet.deserialize(in, oldFormat);
        return new BloomFilter(hashes, bs);
    }
}

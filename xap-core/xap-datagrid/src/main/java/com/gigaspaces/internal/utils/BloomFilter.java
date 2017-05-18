/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
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


package com.gigaspaces.internal.utils;

import java.io.UnsupportedEncodingException;
import java.util.BitSet;


@com.gigaspaces.api.InternalApi
public class BloomFilter {
    private static final int EXCESS = 20;
    private BitSet filter_;

    int hashCount;

    private static MurmurHash hasher = new MurmurHash();

    int getHashCount() {
        return hashCount;
    }

    public int[] getHashBuckets(String key) {
        return getHashBuckets(key, hashCount, buckets());
    }

    public int[] getHashBuckets(byte[] key) {
        return getHashBuckets(key, hashCount, buckets());
    }

    // Murmur is faster than an SHA-based approach and provides as-good
    // collision
    // resistance. The combinatorial generation approach described in
    // https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    static int[] getHashBuckets(String key, int hashCount, int max) {
        byte[] b;
        try {
            b = key.getBytes("UTF-16");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return getHashBuckets(b, hashCount, max);
    }

    static int[] getHashBuckets(byte[] b, int hashCount, int max) {
        int[] result = new int[hashCount];
        int hash1 = hasher.hash(b, b.length, 0);
        int hash2 = hasher.hash(b, b.length, hash1);
        for (int i = 0; i < hashCount; i++) {
            result[i] = Math.abs((hash1 + i * hash2) % max);
        }
        return result;
    }

    BloomFilter(int hashes, BitSet filter) {
        hashCount = hashes;
        filter_ = filter;
    }

    private static BitSet bucketsFor(long numElements, int bucketsPer) {
        long numBits = numElements * bucketsPer + EXCESS;
        return new BitSet((int) Math.min(Integer.MAX_VALUE, numBits));
    }

    /**
     * Calculates the maximum number of buckets per element that this implementation can support.
     * Crucially, it will lower the bucket count if necessary to meet BitSet's size restrictions.
     */
    private static int maxBucketsPerElement(long numElements) {
        numElements = Math.max(1, numElements);
        double v = (Integer.MAX_VALUE - EXCESS) / (double) numElements;
        if (v < 1.0) {
            throw new UnsupportedOperationException("Cannot compute probabilities for "
                    + numElements + " elements.");
        }
        return Math.min(BloomCalculations.probs.length - 1, (int) v);
    }

    /**
     * @return A BloomFilter with the lowest practical false positive probability for the given
     * number of elements.
     */
    public static BloomFilter getFilter(long numElements,
                                        int targetBucketsPerElem) {
        int maxBucketsPerElement = Math.max(1,
                maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem,
                maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem) {
            // logger.warn(String.format("Cannot provide an optimal BloomFilter for %d elements (%d/%d buckets per element).",
            // numElements, bucketsPerElement, targetBucketsPerElem));
        }
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return new BloomFilter(spec.K, bucketsFor(numElements,
                spec.bucketsPerElement));
    }

    /**
     * @return The smallest BloomFilter that can provide the given false positive probability rate
     * for the given number of elements. Asserts that the given probability can be satisfied using
     * this filter.
     */
    public static BloomFilter getFilter(long numElements,
                                        double maxFalsePosProbability) {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        int bucketsPerElement = maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement,
                maxFalsePosProbability);
        return new BloomFilter(spec.K, bucketsFor(numElements,
                spec.bucketsPerElement));
    }

    public void clear() {
        filter_.clear();
    }

    int buckets() {
        return filter_.size();
    }

    BitSet filter() {
        return filter_;
    }

    public boolean isPresent(String key) {
        for (int bucketIndex : getHashBuckets(key)) {
            if (!filter_.get(bucketIndex)) {
                return false;
            }
        }
        return true;
    }

    public boolean isPresent(byte[] key) {
        for (int bucketIndex : getHashBuckets(key)) {
            if (!filter_.get(bucketIndex)) {
                return false;
            }
        }
        return true;
    }

    /*
     * @param key -- value whose hash is used to fill the filter_. This is a
     * general purpose API.
     */
    public void add(String key) {
        for (int bucketIndex : getHashBuckets(key)) {
            filter_.set(bucketIndex);
        }
    }

    public void add(byte[] key) {
        for (int bucketIndex : getHashBuckets(key)) {
            filter_.set(bucketIndex);
        }
    }

    public String toString() {
        return filter_.toString();
    }

    int emptyBuckets() {
        int n = 0;
        for (int i = 0; i < buckets(); i++) {
            if (!filter_.get(i)) {
                n++;
            }
        }
        return n;
    }

    /**
     * @return a BloomFilter that always returns a positive match, for testing
     */
    public static BloomFilter alwaysMatchingBloomFilter() {
        BitSet set = new BitSet(64);
        set.set(0, 64);
        return new BloomFilter(1, set);
    }

    /**
     * This is a very fast, non-cryptographic hash suitable for general hash-based lookup. See
     * http://murmurhash.googlepages.com/ for more details. <p> The C version of MurmurHash 2.0
     * found at that site was ported to Java by Andrzej Bialecki (ab at getopt org). </p>
     */
    private static class MurmurHash {
        public int hash(byte[] data, int length, int seed) {
            int m = 0x5bd1e995;
            int r = 24;

            int h = seed ^ length;

            int len_4 = length >> 2;

            for (int i = 0; i < len_4; i++) {
                int i_4 = i << 2;
                int k = data[i_4 + 3];
                k = k << 8;
                k = k | (data[i_4 + 2] & 0xff);
                k = k << 8;
                k = k | (data[i_4 + 1] & 0xff);
                k = k << 8;
                k = k | (data[i_4 + 0] & 0xff);
                k *= m;
                k ^= k >>> r;
                k *= m;
                h *= m;
                h ^= k;
            }

            // avoid calculating modulo
            int len_m = len_4 << 2;
            int left = length - len_m;

            if (left != 0) {
                if (left >= 3) {
                    h ^= (int) data[length - 3] << 16;
                }
                if (left >= 2) {
                    h ^= (int) data[length - 2] << 8;
                }
                if (left >= 1) {
                    h ^= (int) data[length - 1];
                }

                h *= m;
            }

            h ^= h >>> 13;
            h *= m;
            h ^= h >>> 15;

            return h;
        }
    }

}

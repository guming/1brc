/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * 1. used ByteArrayOutputStream
 * 2. used byte[] and removed hashmap::contains(key)
 * 3. updated key::hashCode and key::equals
 * jprofiler-> bytebuffer.get and hashmap.get ==> poor performance
 * 4th performance improvement with custom HashMap implementation
 */
public class CalculateAverage_gumingcn {

    private static final String FILE = "./measurements.txt";
    private static final byte END_LINE = '\n';
    private static final byte CITY_END_CHAR = ';';
    private static final long DEFAULT_CHUNK_SIZE = 256 * 1024 * 1024;

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public MeasurementAggregator() {
        }

        public void add(double value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count++;
        }

        public String toString() {
            return round(min / 10.0) + "/" + round(sum / (double) (10 * count)) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) {
        Path path = Paths.get(FILE);
        List<ByteBuffer> chunks = new ArrayList<>();
        splitFileIntoChunks(path, chunks);
        List<SimpleHashMap> measurements = chunks.parallelStream().map(CalculateAverage_gumingcn::parseBuffer).toList();
        Map<String, MeasurementAggregator> result = new TreeMap<>();
        for (SimpleHashMap map : measurements) {
            map.merge(result);
        }
        System.out.println(result);
    }

    private static SimpleHashMap parseBuffer(ByteBuffer chunkBytes) {
        int limit = chunkBytes.limit();
        SimpleHashMap measurementMap = new SimpleHashMap(chunkBytes);
        int memOffset = 1;
        // chunkBytes.flip();
        int byteOffset = 0;
        while (chunkBytes.hasRemaining()) {
            int hashCode = 1;
            int keySize = 0;

            while (chunkBytes.position() < limit) {
                byte positionByte = chunkBytes.get();
                if (positionByte == CITY_END_CHAR) {
                    break;
                }
                keySize++;
                hashCode = 31 * hashCode + positionByte;
            }

            boolean negative = false;
            long value = 0;
            int tempOffset = 0;
            while (chunkBytes.position() < limit) {
                byte currentByte = chunkBytes.get();
                tempOffset++;
                if (currentByte == '\n') {
                    break;
                }
                else if (currentByte == '-') {
                    negative = true;
                }
                else if (currentByte != '.') {
                    int digit = currentByte - 48;
                    value = value * 10 + digit;
                }
            }
            if (negative) {
                value = -value;
            }
            measurementMap.put(byteOffset, hashCode, value, keySize, memOffset);
            memOffset += 8;
            byteOffset += tempOffset + keySize + 1;
        }
        return measurementMap;
    }

    private static void splitFileIntoChunks(Path path, List<ByteBuffer> chunks) {

        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);) {
            long from = 0;
            long size = fileChannel.size();
            long chunkSize = Math.max(size / Runtime.getRuntime().availableProcessors(), DEFAULT_CHUNK_SIZE);
            while (from < size) {
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, from, Math.min(chunkSize, size - from));
                while (mappedByteBuffer.hasRemaining()) {
                    int endIndex = mappedByteBuffer.limit() - 1;
                    while (mappedByteBuffer.get(endIndex) != END_LINE) {
                        endIndex--;
                    }
                    endIndex++;
                    mappedByteBuffer.limit(endIndex);
                    from += endIndex;
                    mappedByteBuffer.position(endIndex);
                }
                mappedByteBuffer.position(0);
                chunks.add(mappedByteBuffer);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * the original idea is @merykitty @gonix
     * re-design memory layout
     */
    static class SimpleHashMap {
        ByteBuffer data;// original data
        private static final int STATION_SIZE = 8;// memory layout per station
        private static final int CAPACITY = 1024 * 64;// station size
        private static final int INDEX_MASK = CAPACITY - 1;
        // index[] -> mem[]'s offset
        int[] index;
        // mem[0] = byteOffset + mem[1]=keyLength + mem[2]=measurement + mem[3]=key's hashcode
        // mem[4] ~ mem[7] = count + sum + min + max
        long[] mem;

        public SimpleHashMap(ByteBuffer chunk) {
            index = new int[CAPACITY];
            mem = new long[CAPACITY * STATION_SIZE + 1];
            data = chunk;
        }

        public void put(int byteOffset, int hash, long value, int keyLength, int memOffset) {
            int bucket = hash & INDEX_MASK;
            for (;; bucket = (bucket + 1) & INDEX_MASK) {
                int offset = this.index[bucket];
                if (offset == 0) {
                    this.index[bucket] = memOffset;
                    mem[memOffset] = byteOffset;
                    mem[memOffset + 1] = keyLength;
                    mem[memOffset + 2] = value;
                    mem[memOffset + 3] = hash;
                    mem[memOffset + 4] = 1;// count
                    mem[memOffset + 5] = value;// sum
                    mem[memOffset + 6] = value;// min
                    mem[memOffset + 7] = value;// max
                    break;
                }
                else {
                    int prevKeyLength = (int) mem[offset + 1];
                    int prevHash = (int) mem[offset + 3];
                    if (prevHash == hash && prevKeyLength == keyLength) {
                        mem[offset + 4] += 1;// count
                        mem[offset + 5] += value;// sum
                        mem[offset + 6] = Math.min(value, mem[offset + 6]);// min
                        mem[offset + 7] = Math.max(value, mem[offset + 7]);// max
                        break;
                    }
                }
            }
        }

        public int get(int hash) {
            int bucket = hash & INDEX_MASK;
            bucket = (bucket + 1) & INDEX_MASK;
            return index[bucket];
        }

        void merge(Map<String, MeasurementAggregator> target) {
            this.data.flip();
            for (int i = 0; i < CAPACITY; i++) {
                int offset = this.index[i];
                if (offset == 0) {
                    continue;
                }
                int start = (int) mem[offset];
                int keyLen = (int) mem[offset + 1];

                byte[] keyByte = new byte[keyLen];
                data.get(start, keyByte);
                String key = new String(keyByte, StandardCharsets.UTF_8);
                target.compute(key, (k, v) -> {
                    if (v == null) {
                        v = new MeasurementAggregator();
                    }
                    v.min = Math.min(v.min, mem[offset + 6]);
                    v.max = Math.max(v.max, mem[offset + 7]);
                    v.sum += mem[offset + 5];
                    v.count += mem[offset + 4];
                    return v;
                });
            }
        }
    }
}

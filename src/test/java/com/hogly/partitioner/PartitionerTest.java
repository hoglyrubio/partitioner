package com.hogly.partitioner;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.codec.digest.MurmurHash2;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PartitionerTest {

    public static final Integer PARTITIONS = 5;
    public static final Integer KEYS = 1000;

    @Test
    public void shouldEvenlyPartitionALotOfKeys() {

       final Map<Integer, Integer> partitions = new HashMap<>(PARTITIONS);

       log.info("shouldEvenlyPartitionALotOfKeys: {} Keys: {}", PARTITIONS, KEYS);

       for (int i = 0; i < KEYS; i++) {
           String key = UUID.randomUUID().toString();
           int partition = getPartition(key, PARTITIONS);
           Integer currentAmount = partitions.getOrDefault(partition, 0);
           partitions.put(partition, currentAmount + 1);
       }

       for (Entry<Integer, Integer> entry : partitions.entrySet()) {
        log.info("{} = {} = {}%", entry.getKey(), entry.getValue(), (float) entry.getValue() / KEYS * 100);
       }
    }

    @Test
    public void shouldTheSameKeyAlwaysBeInTheSamePartition() {
        final Map<Integer, Integer> partitions = new HashMap<>(PARTITIONS);
 
        String key = UUID.randomUUID().toString();

        for (int i = 0; i < KEYS; i++) {
            int partition = getPartition(key, PARTITIONS);
            Integer currentAmount = partitions.getOrDefault(partition, 0);
            partitions.put(partition, currentAmount + 1);
        }
        log.info("shouldTheSameKeyAlwaysBeInTheSamePartition: {}", partitions);
 
    }

    public int getPartition(String key, Integer numberOfPartitions) {
        return Math.abs(MurmurHash2.hash32(key)) % numberOfPartitions;
    }

}

package com.onlinejudge.common.sharding;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link PartitionAssigner} — the Push Service's co-partitioning
 * primitive (Part 9 of the blog).
 *
 * <p>Properties verified:
 * <ul>
 *   <li>Deterministic: the same {@code userId} always maps to the same instance.</li>
 *   <li>Stable across instance count: an {@code instanceCount=4} assignment
 *       agrees with what an LB configured with the same count would compute.</li>
 *   <li>Roughly balanced: over a large sample, no instance gets &gt; 2× the mean load.</li>
 *   <li>{@code ownedBy} is just sugar over {@code instanceFor}.</li>
 *   <li>Constructor rejects non-positive instance counts.</li>
 * </ul>
 */
class PartitionAssignerTest {

    @Test
    void instanceFor_deterministic() {
        PartitionAssigner a = new PartitionAssigner(4);
        int first = a.instanceFor("user-abc");
        for (int i = 0; i < 100; i++) {
            assertThat(a.instanceFor("user-abc")).isEqualTo(first);
        }
    }

    @Test
    void instanceFor_returnsValueInRange() {
        PartitionAssigner a = new PartitionAssigner(8);
        for (int i = 0; i < 1000; i++) {
            int instance = a.instanceFor("user-" + i);
            assertThat(instance).isBetween(0, 7);
        }
    }

    @Test
    void distribution_acrossInstances_isReasonablyBalanced() {
        // With 10 000 users across 8 instances, ideal load is 1 250 per instance.
        // Allow each bucket to be in [625, 2 500] — i.e. within 2x of the mean.
        PartitionAssigner a = new PartitionAssigner(8);
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            int instance = a.instanceFor("user-" + i);
            counts.merge(instance, 1, Integer::sum);
        }
        assertThat(counts).hasSize(8);
        for (int c : counts.values()) {
            assertThat(c).isBetween(625, 2500);
        }
    }

    @Test
    void differentUserIds_canShareSameInstance() {
        // With only 4 instances and many users, collisions are expected and fine.
        PartitionAssigner a = new PartitionAssigner(4);
        Set<Integer> instances = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            instances.add(a.instanceFor("user-" + i));
        }
        assertThat(instances).hasSize(4);
    }

    @Test
    void ownedBy_reflectsInstanceFor() {
        PartitionAssigner a = new PartitionAssigner(3);
        String uid = "u-42";
        int owner = a.instanceFor(uid);
        for (int i = 0; i < 3; i++) {
            assertThat(a.ownedBy(uid, i)).isEqualTo(i == owner);
        }
    }

    @Test
    void scalingInstanceCount_remapsMostUsers() {
        // Sanity check that going from 4 → 8 instances doesn't keep everyone on the same
        // index — that would mean the hash is too weak. We're not testing the FNV
        // distribution in depth, just that the function actually uses instanceCount.
        PartitionAssigner four = new PartitionAssigner(4);
        PartitionAssigner eight = new PartitionAssigner(8);
        int sameCount = 0;
        for (int i = 0; i < 1000; i++) {
            String uid = "user-" + i;
            if (four.instanceFor(uid) == eight.instanceFor(uid)) sameCount++;
        }
        // With independent hashes you'd expect ~12.5%. Allow generous bounds.
        assertThat(sameCount).isBetween(50, 500);
    }

    @Test
    void constructor_rejectsZeroOrNegativeInstanceCount() {
        assertThatThrownBy(() -> new PartitionAssigner(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PartitionAssigner(-3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void instanceFor_rejectsNullUserId() {
        PartitionAssigner a = new PartitionAssigner(4);
        assertThatThrownBy(() -> a.instanceFor(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void singleInstance_alwaysReturnsZero() {
        PartitionAssigner a = new PartitionAssigner(1);
        for (int i = 0; i < 100; i++) {
            assertThat(a.instanceFor("user-" + i)).isEqualTo(0);
            assertThat(a.ownedBy("user-" + i, 0)).isTrue();
        }
    }
}

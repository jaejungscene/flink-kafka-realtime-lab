package io.github.jaejungscene.realtimelab.job;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperatorUidsTest {
    @Test
    void keepsEveryStatefulTopologyIdentifierUniqueAndVersioned() {
        List<String> uids = OperatorUids.all();

        assertEquals(uids.size(), new HashSet<>(uids).size());
        assertTrue(uids.stream().allMatch(uid -> uid.matches("[a-z0-9-]+-v[0-9]+")));
    }
}

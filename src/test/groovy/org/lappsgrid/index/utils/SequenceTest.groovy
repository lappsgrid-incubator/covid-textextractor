package org.lappsgrid.index.utils

import org.junit.Test

/**
 *
 */
class SequenceTest {

    @Test
    void simpleTest() {
        Sequence s = new Sequence(1,2)
        assert 1 == s.next()
        assert 2 == s.next()
        assert -1 == s.next()
    }

    @Test
    void invalidSequence() {
        Sequence s = new Sequence(2, 1)
        assert -1 == s.next()
    }

    @Test
    void lengthOneSequence() {
        Sequence s = new Sequence(100,100)
        assert 100 == s.next()
        assert -1 == s.next()
    }

    @Test
    void longSequence() {
        int n = 1_000_000
        int counter = 0;
        Sequence s = new Sequence(1, n)
        while (s.next() > 0) {
            ++counter
        }
        assert counter == n
    }
}

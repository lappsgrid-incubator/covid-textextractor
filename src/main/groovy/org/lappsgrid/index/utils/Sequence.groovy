package org.lappsgrid.index.utils

import java.util.concurrent.atomic.AtomicInteger

/**
 * A thread-safe sequence generator.
 */
class Sequence {
    AtomicInteger n
    final int last

    Sequence(int end) {
        this(1, end)
    }

    Sequence(int start, int end) {
        n = new AtomicInteger(start)
        last = end
    }

    int next() {
        int value = n.getAndIncrement()
        if (value > last) {
            return -1
        }
        return value
    }
}

package org.lappsgrid.index.utils

/**
 *
 */
class SimpleTimer {
    long started
    long stopped
    boolean running

    SimpleTimer() {
        reset()
    }

    SimpleTimer start() {
        if (running) {
            return
        }
        running = true
        started = now()
        return this
    }

    SimpleTimer stop() {
        if (!running) {
            return
        }
        running = false
        stopped = now()
        return this
    }

    SimpleTimer reset() {
        running = false
        started = stopped = 0
        return this
    }

    long time() {
        if (running) {
            return now() - started
        }
        return stopped - started
    }

    String toString() {
        long msec = 0
        long sec = 0
        long min = 0
        long hr = 0

        if (running) {
            msec = now() - started
        }
        else {
            msec = stopped - started
        }
        sec = msec / 1000
        msec = msec % 100
        if (sec > 60) {
            min = sec / 60
            sec = sec % 60
        }
        if (min > 60) {
            hr = min / 60
            min = min % 60
        }
        return String.format("%d:%02d:%02d.%04d", hr, min, sec, msec)
    }

    long now() {
        System.currentTimeMillis()
    }
}

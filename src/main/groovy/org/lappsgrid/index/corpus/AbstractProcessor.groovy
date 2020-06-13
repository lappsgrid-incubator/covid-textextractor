package org.lappsgrid.index.corpus

import groovy.util.logging.Slf4j
import org.lappsgrid.index.api.FileIterator
import org.lappsgrid.index.api.Inserter
import org.lappsgrid.index.api.Processor
import org.lappsgrid.index.model.Fields
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.index.utils.FileSystemIterator
import org.lappsgrid.index.utils.SimpleTimer

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 */
@Slf4j("logger")
abstract class AbstractProcessor implements Processor, Runnable {

    FileSystemIterator files
    Inserter inserter
    int nThreads

    AbstractProcessor(FileIterator files, Inserter inserter, int nThreads) {
        this.files = files
        this.inserter = inserter
        this.nThreads = nThreads

    }

    void run() {
        println "Starting ${nThreads} workers"
        CountDownLatch latch = new CountDownLatch(nThreads)
        AtomicInteger inserted = new AtomicInteger()
        AtomicInteger skipped = new AtomicInteger()
        AtomicInteger nulls = new AtomicInteger()
        AtomicInteger empty = new AtomicInteger()

        SimpleTimer totalTimer = new SimpleTimer()
        totalTimer.start()
        nThreads.times { int i ->
            Thread.start {
                logger.info "Starting thread {}", i
                SimpleTimer timer = new SimpleTimer()
                timer.start()
                try {
                    File file = files.next()
                    while (file != null) {
                        logger.debug "Thread {} {}", i, file.path
                        LappsDocument doc = process(file)
                        int size = doc?.getValue(Fields.BODY)?.size() ?: 0
                        if (doc == null) {
//                            logger.warn("Unable to load document from {}. doc is null", file.path)
                            nulls.incrementAndGet()
                        }
                        else if (size == 0) {
                            logger.warn("No document body in {}", file.path)
                            empty.incrementAndGet()
                        }
                        else if (inserter.insert(doc)) {
                            inserted.incrementAndGet()
                        }
                        else {
                            skipped.incrementAndGet()
                        }
                        file = files.next()
                    }
                }
                catch (Exception e) {
                    logger.error("Processing error.", e)
                }
                latch.countDown()
                timer.stop()
                logger.info "Terminating thread {} after {}", i, timer.toString()
            }
        }
        latch.await()
        inserter.commit()
        totalTimer.stop()
        logger.info "Shutting down."
        logger.info "Total time: {}", totalTimer.toString()
        logger.info("Inserted         : {}", inserted.get())
        logger.info("Null docs        : {}", nulls.get())
        logger.info("Empty docs       : {}", empty.get())
        logger.info("Unable to insert : {}", skipped.get())
        System.exit(0)
    }

    abstract LappsDocument process(File file)

}

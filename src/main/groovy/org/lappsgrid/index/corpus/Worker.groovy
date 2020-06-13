package org.lappsgrid.index.corpus

import groovy.util.logging.Slf4j
import org.lappsgrid.index.api.Extractor
import org.lappsgrid.index.api.FileIterator
import org.lappsgrid.index.api.Inserter
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.index.utils.FileSystemIterator

import java.util.concurrent.CountDownLatch

/**
 *
 */
@Slf4j("logger")
class Worker extends Thread {

    boolean running
    int n
    FileSystemIterator files
    Extractor extractor
    Inserter inserter
    CountDownLatch latch

    Worker(int id, FileIterator files, Extractor extractor, Inserter inserter, CountDownLatch latch) {
        this.n = id
        this.files = files
        this.extractor = extractor
        this.inserter = inserter
        this.latch = latch
    }

    void run() {
        logger.info("Starting worker {}", n)
        running = true
        File file
        while (running && ((file = files.next()) != null)) {
            try {
                LappsDocument document =  extractor.extract(file)
                if (document != null) {
                    inserter.insert(document)
                }
            }
            catch (Exception e) {
                // TODO We need a better way of recording and tracking errors.
                e.printStackTrace()
            }
        }
        inserter.commit()
        logger.info("Thread {} finished.", n)
        latch.countDown()
    }

    void halt() {
        running = false
    }
}

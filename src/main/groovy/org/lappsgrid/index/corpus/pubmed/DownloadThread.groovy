package org.lappsgrid.index.corpus.pubmed


import org.lappsgrid.index.utils.SimpleTimer

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPInputStream

/**
 *
 */
class DownloadThread implements Runnable {
    final File destination = new File("/var/corpora/pubmed/baseline")
    final String ftpurl = "ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    int id
    Sequence sequence
    CountDownLatch latch
    AtomicInteger counter

    DownloadThread(int id, Sequence sequence, CountDownLatch latch, AtomicInteger counter) {
        this.id = id
        this.sequence = sequence
        this.latch = latch
        this.counter = counter
    }

    void run() {
        println "Staring worker $id"
        SimpleTimer timer = new SimpleTimer()
        int n = sequence.next()
        while (n > 0) {
            timer.reset()
            timer.start()
            String name = String.format("pubmed%dn%04d", 20, n)
            String xml = name + ".xml"
            String gz = xml + ".gz"
            String url = ftpurl + gz
            println "Thread $id downloading $gz"
            GZIPInputStream gzIn = new GZIPInputStream(new URL(url).openStream())
            FileOutputStream out = new FileOutputStream(new File(destination, xml))
            byte[] buffer = new byte[4096]
            int len = 0
            int size = 0
            while ((len = gzIn.read(buffer)) > 0) {
                size += len
                out.write(buffer, 0, len)
            }
            out.close()
            gzIn.close()
            timer.stop()
            int sec = timer.time() / 1000
            float rate = (float)size / (float)sec
            String unit = "B/sec"
            if (rate > 1024) {
                rate = rate / 1024.0
                unit = "Kb/sec"
            }
            if (rate > 1024) {
                rate = rate / 1024
                unit = "Mb/sec"
            }
            counter.incrementAndGet()
            printf("Thread %d downloaded %s in %s at %3.3f %s\n", id, xml, timer.toString(), rate, unit)
            n = sequence.next()
        }
        latch.countDown()
        println "Thread $id terminating"
    }
}

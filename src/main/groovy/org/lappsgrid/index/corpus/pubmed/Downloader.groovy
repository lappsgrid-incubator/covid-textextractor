package org.lappsgrid.index.corpus.pubmed

import org.lappsgrid.index.cli.Application
import org.lappsgrid.index.utils.SimpleTimer

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.ParentCommand

/**
 *
 */
@Command(name="pubmed", description = "Add pubmed documents to the Solr index")
class Downloader implements Runnable {

    @ParentCommand
    Application app

    @Option(names = ["-y", "--year"], description = "two digit year to download", paramLabel = "YY", defaultValue = "20")
    int year
    @Option(names=["-s", "--start"], description = "staring index", required = false, defaultValue = "1")
    int start
    @Option(names=["-e", "--end"], description = "ending index (inclusive)", defaultValue = "1015")
    int end

    void run() {
        println "Downloader.run"
        println "Year  : $year"
        println "Start : $start"
        println "End   : $end"

        app.print()
    }

    void download() {
        SimpleTimer timer = new SimpleTimer()
        AtomicInteger total = new AtomicInteger(0);
        timer.start()
        CountDownLatch latch = new CountDownLatch(nThreads)
        Sequence sequence = new Sequence(1,10)
        nThreads.times { n ->
            DownloadThread worker = new DownloadThread(n+1, sequence, latch, total)
            new Thread(worker).start()
        }
        latch.await()
        println "All threads have terminated"
        timer.stop()
        printf "Downloaded %d files in %s\n", total.get(), timer.toString()
    }

    static void main(String[] args) {
        println "Calling Downloader.main()"
    }
}

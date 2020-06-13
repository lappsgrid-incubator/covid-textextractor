package org.lappsgrid.index.corpus.cord19

import groovy.util.logging.Slf4j
import org.lappsgrid.index.solr.SolrInserter
import org.lappsgrid.serialization.Serializer
import org.lappsgrid.index.cli.Application
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.index.utils.FileSystemIterator
import org.lappsgrid.index.utils.SimpleTimer
import picocli.CommandLine.Command
import picocli.CommandLine.ParentCommand

import java.util.concurrent.CountDownLatch

/**
 *
 */
@Slf4j('logger')
@Command(name="cord19", description = "Add CORD19 documents to the Solr index")
class TextExtractor implements Runnable {

    @ParentCommand
    Application app

    // The number of documents processed.
    int count;

    Metadata metadata
    SolrInserter solr //= new SolrInserter()

    TextExtractor() {
        metadata = new Metadata()
        metadata.load()
        count = 0
    }

    void run() {
        logger.info "Processing CORD19 data."
        logger.info "Worker threads {}", app.nThreads
        logger.info "Batch size     {}", app.batchSize

        FileSystemIterator files = new FileSystemIterator(new File(app.indir), app.limit)
        CountDownLatch latch = new CountDownLatch(app.nThreads)
        solr = new SolrInserter(app.solrAddresses, app.core, app.batchSize)

        SimpleTimer totalTimer = new SimpleTimer()
        totalTimer.start()
        app.nThreads.times { int i ->
            Thread.start {
                println "Starting thread $i"
                SimpleTimer timer = new SimpleTimer()
                timer.start()
                try {
                    File file = files.next()
                    while (file != null) {
                        logger.info("Thread {} processing {}", i, file.path)
                        process(file)
                    }
                }
                catch (Exception e) {
                    logger.error("Thread {} encountered a problem.", i, e)
                }
                latch.countDown()
                timer.stop()
                println "Terminating thread $i after ${timer.toString()}"
            }
        }
        latch.await()
        solr.commit()
        totalTimer.stop()
        println "Shutting down."
        println "Total time: ${totalTimer.toString()}"
    }

    void process(File file) {
        logger.debug("Processing {}", file.path)
        Map document = Serializer.parse(file.text, Map)
        Map entry = metadata.lookup(document.paper_id)
        if (entry == null) {
            logger.warn("No metadata for {}", document.paper_id)
        }
        else {
            LappsDocument lapps = new LappsDocument()
                    .id(document.paper_id)
                    .pmc(entry.pmcid)
                    .pmid(entry.pubmed_id)
//                    .doi(entry.doi)
                    .title(entry.title)
                    .articleAbstract(entry.abstract)
                    .body(collectText(document))
                    .journal(entry.journal)
            solr.insert(lapps)
        }

    }
    String collectText(Map document) {
        StringWriter writer = new StringWriter();
        PrintWriter printer = new PrintWriter(writer);
        document.body_text.each { section ->
            printer.println(section.section)
            printer.println(section.text)
            printer.println()
        }
        return writer.toString()
    }


    static void main(String[] args) {
//        new TextExtractor().process("/var/corpora/covid/comm_use_subset/pmc_json/PMC7107008.xml.json")
        println "TextExtractor.main"
    }
}

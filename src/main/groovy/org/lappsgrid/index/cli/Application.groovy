package org.lappsgrid.index.cli


import org.lappsgrid.index.Version
import org.lappsgrid.index.api.FileIterator
import org.lappsgrid.index.corpus.Worker
import org.lappsgrid.index.corpus.cord19.CordExtractor
import org.lappsgrid.index.corpus.pubmed.PMCExtractor
import org.lappsgrid.index.elastic.ElasticInserter
import org.lappsgrid.index.solr.SolrInserter
import org.lappsgrid.index.utils.FileSystemIterator
import org.lappsgrid.index.utils.MetadataFileIterator
import org.lappsgrid.index.utils.SimpleTimer
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.ArgGroup

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 */
@Command(name="java -jar index.jar", description = "%nAdd documents to a Solr or ElasticSearch index",
        sortOptions = false, versionProvider = VersionProvider)
class Application implements Runnable {

    @Option(names=["-i", "--input"], description = "directory to process", paramLabel = "DIR", required = false)
    File indir

    @Option(names=["-b", "--batch-size"], description = "how many documents to process between batch commits", paramLabel = "<N>", defaultValue = "1000")
    int batchSize

    @Option(names=["-t", "--threads"], description = "number of worker threads", paramLabel = "<N>", defaultValue = "1")
    int nThreads

    @Option(names=["-c", "--core"], description = "the Solr collection or ElasticSearch index the documents will be added to", required = false, defaultValue = 'covid19')
    String core = "covid19"

    @Option(names=["-m", "--meta"], description = "location of the metadata.csv file", paramLabel = "<FILE>", required = false)
    File metadataFile

    static class SearchEngine {
        @Option(names=["--solr"], description = "address(es) of the Solr Cloud instances.", arity="1..*", paramLabel = '<IP>', required = false)
        List solr //= ['http://localhost:8983/solr' ]
        @Option(names=["--elastic"], description = "address(es) of the ElasticSearch nodes.", arity = "1..*", paramLabel = "<IP>", required = false)
        List elastic //= ['http://localhost:9200']
    }
    @ArgGroup(heading = "Search Engine%n", multiplicity = "1")
    SearchEngine search;

    @Option(names=['-l', '--limit'], description = "only process this number of files", arity = "1", required = false, defaultValue = "-1")
    int limit

    static class FileTypeGroup {
        @Option(names='--cord19', description = "process CORD19 json files")
        boolean cord19
        @Option(names="--pubmed", description = "process PubMed xml files")
        boolean pubmed
        @Option(names="--pmc", description = "process PubMed Central xml files")
        boolean pmc
    }
    @ArgGroup(heading = "Input File Type%n", multiplicity = "1")
    FileTypeGroup type

    @Option(names=['-h','--help'], description = 'show this help and exit', usageHelp = true, order = 100)
    boolean showHelp

    @Option(names = ["-v", "--version"], description = "show application version number", versionHelp = true, order = 99)
    boolean showVersion

    void run() {
        println("Limiting run to ${limit} files")
//        FileIterator files = new FileSystemIterator(new File(indir), FileSystemIterator.JSON, limit)
        FileIterator files = null
        Closure createInserter
        Closure createExtractor

        AtomicInteger extracted = new AtomicInteger()
        AtomicInteger inserted = new AtomicInteger()

        if (search.solr != null && search.solr.size() > 0) {
            println("Creating a Solr collection")
            createInserter = { SolrInserter.Http(search.solr[0], core, batchSize, inserted) }
        }
        else if (search.elastic != null && search.elastic.size() > 0) {
            println("Creating an ElasticSearch index")
            createInserter = { new ElasticInserter(search.elastic, core, batchSize, inserted) }
        }
        else {
            println("Missing search engine type.")
            return
        }

        if (type.cord19) {
            if (metadataFile == null) {
                println "The metadata.csv file must be specified."
                return
            }
            if (!metadataFile.exists()) {
                println "The metadata.csv can not be found."
                return
            }
            files = new MetadataFileIterator(metadataFile, limit)
            createExtractor= { new CordExtractor(extracted, metadataFile) }
        }
        else if (type.pmc) {
            if (indir == null) {
                println "No input directory specified."
                return
            }
            if (!indir.exists()) {
                println "Input directory does not exist."
                return
            }
            createExtractor = { new PMCExtractor(extracted) }
            files = new FileSystemIterator(indir, FileSystemIterator.JSON, limit)
        }
        else if (type.pubmed) {
            println "That corpus type is not supported yet."
            return
        }
        else {
            println("Missing corpus type.")
            return
        }

        SimpleTimer timer = new SimpleTimer()
        CountDownLatch latch = new CountDownLatch(nThreads)
        List<Worker> workers = []
        for (int i = 0; i < nThreads; ++i) {
            workers.add(new Worker(i, files, createExtractor(), createInserter(), latch))
        }

        println "Starting ${nThreads} threads"
        timer.start()
        workers*.start()
        println "Waiting for all threads to terminate."
        workers*.join()
        timer.stop()
//        latch.await()
        println "Done."
        println "Inserted ${inserted.get()} documents in ${timer}"
        System.exit(0)
    }

    void print() {
        println "Application Settings"
        println "Directory  : ${indir}"
        println "Batch size : ${batchSize}"
        println "Workers    : ${nThreads}"
        println "Collection : ${core}"
        println "Servers    : [ ${solrAddresses.join(', ')} ]"
    }

    static void main(String[] args) {
        new CommandLine(new Application()).execute(args)
    }

    static void _main(String[] args) {
        Application app = new Application()
//        new CommandLine(app).parse(args)
        CommandLine cli = new CommandLine(app)
        try {
            cli.parseArgs(args)
        }
        catch (Exception e) {
            println()
            println e.message
            println()
            cli.usage(System.out)
            println()
            return
        }
        if (app.usageHelp) {
            println()
            cli.usage(System.out)
            println()
            return
        }

        if (app.versionHelp) {
            println "\nLappsgrid SolrInserter v" + Version.version
            println "Copyright 2020 The Lanugage Applications Grid\n"
            return
        }
        println "Something should be invoked here..."
    }
}

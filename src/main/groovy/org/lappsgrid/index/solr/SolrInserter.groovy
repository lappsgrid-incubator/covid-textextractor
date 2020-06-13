package org.lappsgrid.index.solr

import groovy.util.logging.Slf4j
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.SolrInputDocument
import org.codehaus.janino.Java.Atom
import org.lappsgrid.index.api.Inserter
import org.lappsgrid.index.model.LappsDocument

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

/**
 *
 */
@Slf4j('logger')
class SolrInserter implements Inserter {
    final static int BATCH_SIZE = 1000
    SolrClient solr
    final int batchSize
    AtomicInteger count
    final String core;
//    final Lock lock;

    Collection<SolrDocument> cache

    SolrInserter(String[] servers, String core) {
        this(servers, core, BATCH_SIZE)
    }

    SolrInserter(SolrClient solr, String core, int interval, AtomicInteger counter) {
        this.solr = solr
        this.batchSize = interval
        this.count = counter
        this.core = core
        this.cache = new ConcurrentLinkedQueue<>()
        this.lock = new ReentrantLock()
//        logger.debug("Core: {}", core)
//        logger.debug("Size: {}", interval)
    }

    static SolrInserter Http(String server, String core, int interval, AtomicInteger counter) {
        SolrClient client = new HttpSolrClient.Builder(server).build()
        return new SolrInserter(client, core, interval, counter)
    }

    static SolrInserter Cloud(List<String> servers, String core, int interval, AtomicInteger counter) {
        SolrClient client = new CloudSolrClient.Builder(servers).build()
        return new SolrInserter(client, core, interval, counter)
    }

    boolean insert(LappsDocument document) {
        if (document == null) {
            return false
        }
        SolrInputDocument solr = new SolrInputDocument()
        document.values.each { name, value ->
            solr.setField(name, value)
        }
        return insert(solr)
    }

    boolean insert(SolrInputDocument document) {
        int n = count.incrementAndGet()
        logger.trace("{}. Inserting document {}", n, document.getFieldValue("id"))
        cache.add(document)
        if (cache.size() >= batchSize) {
            logger.debug("Posting batch to solr: {} documents", cache.size())
            try {
                solr.add(core, cache)
            }
            catch (Exception e) {
                logger.error e.message
                return false
            }
            finally {
                cache.clear()
            }
        }
        return true
    }

    void commit() {
        logger.info("Committing documents to solr")
        if (cache.size() > 0) {
            logger.debug("Adding final batch: {}", cache.size())
            solr.add(core, cache)
        }
        solr.commit(core)
    }
}

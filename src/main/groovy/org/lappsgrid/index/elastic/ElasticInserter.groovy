package org.lappsgrid.index.elastic

import groovy.util.logging.Slf4j
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.lappsgrid.index.api.Inserter
import org.lappsgrid.index.model.LappsDocument

import java.util.concurrent.atomic.AtomicInteger

/**
 *
 */
@Slf4j("logger")
class ElasticInserter implements Inserter {

    List<LappsDocument> cache = [].asSynchronized()
    final int batchSize
    RestHighLevelClient client
    final String index
    AtomicInteger counter

    ElasticInserter(List<String> addresses, String index, int batchSize, AtomicInteger counter) {
        HttpHost[] hosts = new HttpHost[addresses.size()]
        addresses.collect{ it ->
            URL url = new URL(it)
            new HttpHost(url.host, url.port, url.protocol)
        }.toArray(hosts)

        client = new RestHighLevelClient( RestClient.builder(hosts) )
        this.batchSize = batchSize
        this.index = index
        this.counter = counter
    }

    @Override
    boolean insert(LappsDocument document) {
        if (document == null) {
            return false
        }
        counter.incrementAndGet()
        cache.add(document)
        if (cache.size() >= batchSize) {
            send()
            logger.trace("Update complete.")
        }
        return true
    }

    @Override
    void commit() {
        if (cache.size() > 0) {
            BulkRequest request = prepare()
            if (request) {
                send(request)
            }

        }
    }

    synchronized BulkRequest prepare() {
        if (cache.size() == 0) {
            return null
        }

        BulkRequest bulk = new BulkRequest()
        cache.each { LappsDocument doc ->
            IndexRequest request = new IndexRequest(index).source(doc.fields())
            bulk.add(request)
        }
        cache.clear()
        return bulk
    }

    synchronized boolean send(BulkRequest bulk) {
        try {
            BulkResponse response = client.bulk(bulk, RequestOptions.DEFAULT)
            if (response.hasFailures()) {
                logger.warn("There were failures during the bulk update.")
                logger.warn(response.buildFailureMessage())
                return false
            }
            else {
                logger.debug("{} BulkResponse code: {}", counter, response.status().status)
            }
        }
        catch (Exception e) {
            logger.error("Unable to perform bulk upload.", e)
            return false
        }
        return true
    }

//    @Slf4j("logger")
//    class BatchThread implements Runnable {
//        List<LappsDocument> documents
//        RestHighLevelClient client
//        final String index
//
//        BatchThread(List<LappsDocument> documents, RestHighLevelClient client, String index) {
//            this.documents = documents
//            this.client = client
//            this.index = index
//        }
//
//        void run() {
//            logger.info("Staring the BatchThread")
//            BulkRequest bulk = new BulkRequest()
//            documents.each { LappsDocument doc ->
//                IndexRequest request = new IndexRequest(index).source(doc.fields())
//                bulk.add(request)
//            }
//            try {
//                BulkResponse response = client.bulk(bulk, RequestOptions.DEFAULT)
//                if (response.hasFailures()) {
//                    logger.warn("There were failures during the bulk update.")
//                    logger.warn(response.buildFailureMessage())
//                }
//                else {
//                    logger.debug("BulkResponse code: {}", response.status().status)
//                }
//            }
//            catch (Exception e) {
//                logger.error("Unable to perform bulk upload.", e)
//            }
//            logger.trace("Update complete.")
//        }
//
//    }
}

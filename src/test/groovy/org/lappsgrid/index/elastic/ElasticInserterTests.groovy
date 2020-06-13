package org.lappsgrid.index.elastic

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.lappsgrid.index.corpus.cord19.Cord19Processor
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.serialization.Serializer

/**
 *
 */
@Ignore
class ElasticInserterTests {

    String INDEX = "cord19-2020-04-24"
    RestHighLevelClient client

    @Before
    void setup() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        )
    }

    @After
    void teardown() {
        client.close()
        client = null
    }

    @Test
    void simple() {
        InputStream stream = this.class.getResourceAsStream("/PMC6031596.xml.json")
        assert stream != null


        try {
            Cord19Processor extractor = new Cord19Processor(null, null, 1)
            LappsDocument document = extractor.process(stream)

            IndexRequest request = new IndexRequest(INDEX)
            request.source(document.fields())
            IndexResponse response = client.index(request, RequestOptions.DEFAULT)
            println response.status().toString()
            println response.getResult().toString()
        }
        finally {
            client.close()
        }
    }

    @Test
    void search() {
        SearchRequest request = new SearchRequest()
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        QueryBuilder q;
        q.
        request.source(source).indices(INDEX)

        SearchResponse response = client.search(request, RequestOptions.DEFAULT)
        SearchHit[] hits = response.hits.hits
        assert 1 == response.hits.size()
        for (SearchHit hit : hits) {
            println Serializer.toPrettyJson(hit)
        }
    }
}

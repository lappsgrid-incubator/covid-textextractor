package org.lappsgrid.index.corpus.cord19

import org.apache.solr.common.SolrDocument
import org.junit.Test
import org.lappsgrid.askme.core.api.Packet
import org.lappsgrid.askme.core.api.Query
import org.lappsgrid.askme.core.concurrent.Signal
import org.lappsgrid.askme.core.model.Document
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.rabbitmq.Message
import org.lappsgrid.rabbitmq.topic.MessageBox
import org.lappsgrid.rabbitmq.topic.PostOffice
import org.lappsgrid.serialization.Serializer

import java.util.concurrent.atomic.AtomicInteger

/**
 *
 */
class ScoreSingleDocument {

    static final String ADDRESS = "sample-doc-ranking"
    static final String ROOT = "/var/corpora/covid/2020-06-28"
    final CordExtractor extractor
    final AtomicInteger counter
    final Metadata md
    Stanford nlp

    ScoreSingleDocument() {
        counter = new AtomicInteger()
        println "Loading metadata"
        md = new Metadata().load(new File(ROOT, "metadata-fixed.csv"))
        extractor = new CordExtractor(counter, md)
        println "Initializing NLP"
        nlp = new Stanford()
    }

    void vespa() {
        String input ='''https://doi.org/10.1186/1743-422x-2-69
https://doi.org/10.1007/978-981-15-4814-7_3
https://doi.org/10.1101/2020.06.26.174698
'''
//https://doi.org/10.1016/j.ijantimicag.2020.105938
//https://doi.org/10.1101/2020.04.03.023846
//https://doi.org/10.1111/jcmm.15312
//https://doi.org/10.1016/j.virol.2009.09.007
//https://doi.org/10.1186/1743-422x-2-69
//https://doi.org/10.1101/2020.04.24.20078741
//https://doi.org/10.1016/j.phrs.2020.104904
//https://doi.org/10.1161/circep.120.008662
//https://doi.org/10.1093/ofid/ofaa130
//https://doi.org/10.1016/j.antiviral.2013.04.016
//https://doi.org/10.1016/j.antiviral.2013.04.016
//https://doi.org/10.1016/j.ijantimicag.2020.105960'''

        List ids = input.readLines().collect{ it.substring(16).trim() }
        processIdList(ids)
    }

    void litcovid() {
//        Metadata md = new Metadata().load("/Users/suderman/Desktop/metadata-fixed.csv")
        String s = '''// 1. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32171740
        // 2. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32579059        
        // 3. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32408070
        // 4. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32458149
        // 5. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32173110
        // 6. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32375574
        // 7. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32564047
        // 8. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32373993
        // 9. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32473812
        //10. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32448818'''
        String s1 = '''// 9. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32473812
        //10. https://www.ncbi.nlm.nih.gov/research/coronavirus/publication/32448818'''

        println "Loading documents"
        List idList = s.readLines().collect { it.split('/')[-1].trim() }
        processIdList(idList)
    }

    void litcovid2() {
        String input = '''PMC7118659
PMC7202847
PMC7250542
PMC7270792
PMC7232887
PMC7244425
PMC7255230
PMC7249615
PMC7108130
PMC7275144'''
        List ids = input.readLines().collect{ it.trim() }
        processIdList(ids)
    }

    void printLitCovid() {
        String input = '''PMC7118659
PMC7202847
PMC7250542
PMC7270792
PMC7232887
PMC7244425
PMC7255230
PMC7249615
PMC7108130
PMC7275144'''
        List ids = input.readLines().collect{ it.trim() }
        ids.eachWithIndex{ String id, int i ->
            Map entry = md.lookup(id)
            printf "%2d\t%s\t%s\n", (i+1), entry.pmcid, entry.title
        }
    }
    void processIdList(List idList) {
        List<Document> docs = []
        idList.eachWithIndex {  id, i ->
            Map entry = md.lookup(id)
            if (entry) {
                Document doc = loadFile(entry)
                if (doc) {
                    docs.add(doc)
                }
                else {
                    println "Unable to load document for $id"
                }
            }
            else {
                println "${i+1} No entry for ID $id found."
            }
        }
        if (docs.size() == 0) {
            println "No documents found"
            return
        }
        Packet packet = new Packet()
        packet.core = "cord_2020_06_12"

        Map m = [
            "question" : "What is the effect of chloroquine on SARS-Cov-2 replication?",
            "query" : "body:effect AND body:chloroquine AND (body:coronavirus or body:covid or body:sars-cov-2) AND body:replication",
            "terms" : [ "effect", "chloroquine", "sars-cov-2", "replication" ],
            "count" : 100
        ]
        packet.query = new Query(m)
        packet.documents = docs
        Signal signal = new Signal()
        println "Creating mailbox"
        MessageBox box = new MessageBox("askme_dev", ADDRESS, "rabbitmq.lappsgrid.org/dev") {
            @Override
            void recv(Message message) {
                new File("/tmp/scored-results.json").text = Serializer.toPrettyJson(message.body)
                printResults(message.body)
                signal.send()
            }
        }

        printf "Sending message"
        Message message = new Message().command("ok").body(packet).route("ranking.mailbox", ADDRESS)
        message.setParameters(params())
        PostOffice po = new PostOffice("askme_dev", "rabbitmq.lappsgrid.org/dev")
        po.send(message)
        println "Waiting for results"
        signal.await()
        println "Shutting down"
        po.close()
        box.close()
        println "Done"
    }

    Document loadFile(Map entry) {
        String path = entry.pmc_json_files ?: entry.pdf_json_files
        println "Path is $path"
        File file = new File(ROOT, path)
        if (file.exists()) {
            println "loading ${file.path}"
            return createDocument(extractor.extract(file))
        }
        else {
            println "File not found ${file.path}"
        }
        return null
    }

    Document createDocument(LappsDocument solr){
        println Serializer.toPrettyJson(solr)
        Document document = new Document()
        ['id', 'pmid', 'pmc', 'doi', 'year', 'url', 'path'].each { field ->
            document[field] = solr.values[field]
        }

        println "Running Stanford CoreNLP"
        document.title = nlp.process(solr.title())
        document.articleAbstract = nlp.process(solr.articleAbstract())
        return document
    }

    void printResults(Map data) {
        data.documents.each { Map doc ->
            printf "%s\t%2.3f\t%s\n", id(doc), doc.score, doc.title.text
        }
    }

    String id(Map item) {
        return item.pmc
//        ['pmc', 'pmid', 'doi'].each {
//            if (item[it]) return item[it]
//        }
//        Map entry = md.lookup(item.id)
//        ['pmc', 'pmid', 'doi'].each {
//            if (entry[it]) return entry[it]
//        }
//        return item.id
    }

    static void main(String[] args) {
        System.setProperty("RABBIT_USERNAME","developer")
        System.setProperty("RABBIT_PASSWORD","eqg5BePVYDB%rK.E")
        new ScoreSingleDocument().printLitCovid()
    }

    Map params() {
        String json = '''{
            "domain" : "cord_2020_06_12",
            "question" : "What is the effect of chloroquine on SARS-Cov-2 replication?",
            "title-checkbox-1" : "1",
            "title-weight-1" : "1.0",
            "title-checkbox-2" : "2",
            "title-weight-2" : "1.0",
            "title-checkbox-3" : "3",
            "title-weight-3" : "1.0",
            "title-checkbox-4" : "4",
            "title-weight-4" : "1.0",
            "title-checkbox-5" : "5",
            "title-weight-5" : "1.0",
            "title-checkbox-6" : "6",
            "title-weight-6" : "1.0",
            "title-checkbox-7" : "7",
            "title-weight-7" : "1.0",
            "title-weight-x" : "0.9",
            "abstract-checkbox-1" : "1",
            "abstract-weight-1" : "1.0",
            "abstract-checkbox-2" : "2",
            "abstract-weight-2" : "1.0",
            "abstract-checkbox-3" : "3",
            "abstract-weight-3" : "1.0",
            "abstract-checkbox-4" : "4",
            "abstract-weight-4" : "1.0",
            "abstract-checkbox-5" : "5",
            "abstract-weight-5" : "1.0",
            "abstract-checkbox-6" : "6",
            "abstract-weight-6" : "1.0",
            "abstract-checkbox-7" : "7",
            "abstract-weight-7" : "1.0",
            "abstract-weight-x" : "1.1"
        }'''
        return Serializer.parse(json, HashMap)
    }

}

package org.lappsgrid.index.corpus.pubmed

import org.lappsgrid.index.api.Extractor
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.index.utils.Factory

import java.util.concurrent.atomic.AtomicInteger

/**
 *
 */
class PMCExtractor implements Extractor {

    XmlParser parser
    AtomicInteger counter

    PMCExtractor(AtomicInteger counter) {
        this.counter = counter
        parser = Factory.createXmlParser()
    }

    public LappsDocument extract(File file) {
        LappsDocument doc = extract(file.newInputStream())
        if (doc != null) {
            counter.incrementAndGet()
            doc.path(file.path)
        }
        return doc
    }

    public LappsDocument extract(InputStream stream) {
        return extract(parser.parse(stream))
    }

    public LappsDocument extract(Node article) {
        Node front = article.front[0]
        Node meta = front.'article-meta'[0]
        String journal = front.'journal-meta'.'journal-title-group'.'journal-title'.text()
        String pmid = getIdValue(meta, 'pmid')
        String pmc = getIdValue(meta, 'pmc')
        String doi = getIdValue(meta, 'doi')
        String title = meta.'title-group'.'article-title'.text()
        String year = '0'
        Node pubDate = meta.'pub-date'.find { it.@'pub-type' == 'ppub' }
        if (pubDate == null) {
            pubDate = meta.'pub-date'.find { it.@'pub-type' == 'epub' }
        }
        if (pubDate != null) {
            year = pubDate.year.text()
        }
        title = normalize(title)

        List keywords = []
        meta.'kwd-group'.kwd.each { kwd ->
            keywords << normalize(kwd)
        }

        def bodyNode = article.body
        String body = collectBody(bodyNode)
        LappsDocument document
        try {
            document = new LappsDocument()
                    .id(getId(pmid, pmc, doi))
                    .pmid(pmid)
                    .pmc(pmc)
                    .doi(doi)
                    .journal(journal)
                    .title(title)
                    .year(year.trim())
                    .keywords(keywords.join(", "))
                    .body(body)
                    .theAbstract(meta.abstract.text())
                    .introduction(collectSection("intro", bodyNode))
                    .results(collectSection("result", bodyNode))
                    .discussion(collectSection('discuss', bodyNode))
                    .url("https://www.ncbi.nlm.nih.gov/pmc/articles/${pmc}/?report=classic")

        }
        catch (Exception e) {
            e.printStackTrace()
            document = new LappsDocument()
        }
        return document
    }

    String collectBody(NodeList nodes) {
        StringWriter writer = new StringWriter()
        PrintWriter printer = new PrintWriter(writer)
        nodes.each { node ->
            node.sec.each { section ->
                printer.println(section.title.text())
                section.p.each { paragraph ->
                    printer.println(paragraph.text())
                }
            }
        }
        return writer.toString()
    }


    String collectBody(Node node) {
        StringWriter writer = new StringWriter()
        PrintWriter printer = new PrintWriter(writer)
        node.sec.each { section ->
            printer.println(section.title.text())
            section.p.each { paragraph ->
                printer.println(paragraph.text())
            }
        }
        return writer.toString()
    }

    String collectSection(String type, NodeList nodes) {
        StringWriter writer = new StringWriter()
        PrintWriter printer = new PrintWriter(writer)
        nodes.each { node ->
            node.sec.each { section ->
                String secType = section.attribute('sec-type')
                if (secType && secType.startsWith(type)) {
                    printer.println(section.title.text())
                    section.p.each { paragraph ->
                        printer.println(paragraph.text())
                    }
                }
            }
        }
        return writer.toString()
    }

    String getIdValue(Node node, String id) {
        Node result = node.'article-id'.find { it.@'pub-id-type' == id }
        if (result == null) {
            return null
        }
        def value = result.value()
        if (value == null) {
            return null
        }
        return value[0]
    }
}

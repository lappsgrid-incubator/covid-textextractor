package org.lappsgrid.index.corpus.cord19

import groovy.util.logging.Slf4j
import org.lappsgrid.index.api.Extractor
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.serialization.Serializer

import java.util.concurrent.atomic.AtomicInteger

/**
 *
 */
@Slf4j("logger")
class CordExtractor implements Extractor {

    private Metadata metadata
    private AtomicInteger counter

    CordExtractor(AtomicInteger counter, Metadata md) {
        this.counter = counter
        metadata = md
    }

    LappsDocument extract(File file) {
        String path = file.path
        logger.debug("Processing {}", path)
        LappsDocument doc = extract(file.text)
        if (doc == null) {
            return doc
        }

        doc.path(path)
//        if (path.contains('/comm_use_subset/')) {
//            doc.license('comm')
//        }
//        else if (path.contains("/noncomm_use_subset/")) {
//            doc.license('noncomm')
//        }
//        else if (path.contains('/custom_license/')) {
//            doc.license('custom')
//        }
//        else if (path.contains('/biorxiv_medrxiv/')) {
//            doc.license('rxiv')
//        }
//        else {
//            doc.license('unknown')
//        }
        counter.incrementAndGet()
        return doc
    }

    LappsDocument extract(InputStream stream) {
        return extract(stream.text)
    }

    LappsDocument extract(String json) {
        Map document = Serializer.parse(json, Map)
        Map entry = metadata.lookup(document.paper_id)
        if (entry == null) {
            entry = metadata.findBySha(document.paper_id)
        }
        if (entry == null) {
            logger.warn("No metadata for {}", document.paper_id)
            return null
        }
        String text = collectText(document)
        if (text.length() == 0) {
            logger.debug("Document {} is empty.", document.paper_id)
            return null
        }

        LappsDocument lapps = new LappsDocument()
                .id(document.paper_id)
                .pmc(entry.pmcid)
                .pmid(entry.pubmed_id)
                .doi(entry.doi)
                .title(entry.title)
                .articleAbstract(entry.abstract)
                .body(text)
                .journal(entry.journal)
                .year(entry.publish_time.split('-')[0])
                .author(entry.authors)
                .license(entry.license ?: 'unknown')
                .url(entry.url)

        return lapps
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

}

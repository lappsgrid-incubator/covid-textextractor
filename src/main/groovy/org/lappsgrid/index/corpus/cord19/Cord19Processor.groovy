package org.lappsgrid.index.corpus.cord19

import groovy.util.logging.Slf4j
import org.lappsgrid.index.api.FileIterator
import org.lappsgrid.serialization.Serializer
import org.lappsgrid.index.api.Inserter
import org.lappsgrid.index.corpus.AbstractProcessor
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.index.utils.FileSystemIterator

/**
 * @deprecated
 */
@Deprecated
@Slf4j("logger")
class Cord19Processor extends AbstractProcessor {
    Metadata metadata

    Cord19Processor(FileIterator files, Inserter solr, int nThreads = 1) {
        super(files, solr, nThreads)
        metadata = new Metadata()
        metadata.load()
    }

    LappsDocument process(InputStream stream) {
        logger.debug("Processing InputStream")
        return this.process(stream.text)
    }

    LappsDocument process(File file) {
        String path = file.path
        logger.debug("Processing {}", path)
        LappsDocument doc = process(file.text)
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

        return doc
    }

    LappsDocument  process(String json) {
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

//        String id = getId(entry)
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
                .author(entry.author)
                .license(entry.license ?: 'unknown')
                .url(entry.url)

        return lapps
    }

    String getId(Map entry) {
        // Look for ID candidates and use the first one found.
        ['paper_id', 'pubmed_id', 'pmcid', 'doi', 'sha', 'cord_uid'].each { String key ->
            if (entry[key]) {
                return entry[key]
            }
        }
        return UUID.randomUUID().toString()
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

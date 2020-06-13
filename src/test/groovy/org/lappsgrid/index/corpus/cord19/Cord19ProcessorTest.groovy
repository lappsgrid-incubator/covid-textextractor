package org.lappsgrid.index.corpus.cord19

import org.junit.Ignore
import org.junit.Test
import org.lappsgrid.serialization.Serializer
import org.lappsgrid.index.api.Inserter
import org.lappsgrid.index.api.Processor
import org.lappsgrid.index.model.Fields
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.index.utils.FileSystemIterator

/**
 *
 */
@Ignore
class Cord19ProcessorTest {

    @Test
    void parseAll() {
        File cord = new File('/var/corpora/covid/noncomm_use_subset/pmc_json/')
        FileSystemIterator files = new FileSystemIterator(cord, 2)
        Inserter inserter = new MockInserter()
        Processor processor = new Cord19Processor(files, inserter, 1)
        processor.run()
        inserter.commit()
    }
}


class MockInserter implements Inserter {

    int titles
    int abstracts
    int bodies

    @Override
    boolean insert(LappsDocument document) {
        String id = document.getValue(Fields.ID)
        println "Inserting document ${id}"
        String title = document.title()
        String theAbstract = document.articleAbstract()
        String body = document.body()
        if (title != null) ++titles
        if (theAbstract != null) ++ abstracts
        if (bodies != null) ++bodies
        File outfile = new File("/tmp/covid/${id}.json")
        outfile.text = Serializer.toPrettyJson(document)
        println "Wroter ${outfile.path}"
        return true
    }

    @Override
    void commit() {
        println "Titles ${titles}"
        println "Abstracts ${abstracts}"
        println "Bodies $bodies"
    }
}
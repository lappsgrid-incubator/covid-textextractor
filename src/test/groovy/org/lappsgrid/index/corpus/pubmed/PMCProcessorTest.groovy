package org.lappsgrid.index.corpus.pubmed

import org.junit.Test
import org.lappsgrid.index.api.Processor
import org.lappsgrid.index.model.Fields
import org.lappsgrid.index.model.LappsDocument

/**
 *
 */
class PMCProcessorTest {

    @Test
    void streamTest() {
        InputStream stream = this.class.getResourceAsStream("/20256506.xml")
        String xml =  stream.text
        println xml.size()

    }
    @Test
    void parseTest() {
        InputStream stream = this.class.getResourceAsStream("/20256506.xml")
        Processor p = new PubmedProcessor(null, null, 0)
        LappsDocument d = p.process(stream)
        assert d != null
        //println d.toString()
        println d.getValue(Fields.TITLE)
        println d.getValue(Fields.ABSTRACT)
        println d.getValue(Fields.BODY)
    }
}

package org.lappsgrid.index.corpus.pubmed

import org.lappsgrid.index.api.Inserter
import org.lappsgrid.index.corpus.AbstractProcessor
import org.lappsgrid.index.model.LappsDocument
import org.lappsgrid.index.utils.FileSystemIterator
import org.lappsgrid.index.utils.Factory

/**
 *
 */
class PubmedProcessor extends AbstractProcessor {

    XmlParser parser

    PubmedProcessor(FileSystemIterator files, Inserter solr, int nThreads) {
        super(files, solr, nThreads)
        parser = Factory.createXmlParser()
    }

    LappsDocument process(File file) {
        process(file.newInputStream())
    }

    LappsDocument process(InputStream stream) {
        return extractValues(parser.parse(stream))
    }

    LappsDocument process(Reader reader) {
        return extractValues(parser.parse(reader))
    }

    LappsDocument extractValues(Node pubmed) {
        Node medline = pubmed.MedlineCitation[0]
        def article = medline.Article[0]
        String title = article.ArticleTitle.text()
        String articleAbstract = article.Abstract.AbstractText.text()
        def journal = article.Journal
        //String journal = article.Journal.Title.text()
        String year = journal.JournalIssue.PubDate.Year.text()
        Node pmid = medline.PMID[0]
        //PubmedArticle.PubmedData.ArticleIdList.ArticleId[@IdType = 'pmc'
        def data = pubmed.PubmedData
        Node pmc = data.ArticleIdList.ArticleId.find { it.@IdType == 'pmc' }

        List<String> mesh = []
        medline.MeshHeadingList.MeshHeading.each { Node heading ->
            mesh.add(heading.DescriptorName.text())
        }

        LappsDocument document = new LappsDocument()
                .title(title)
                .theAbstract(articleAbstract)
                .journal(journal.Title.text())
                .year(year)
                .id(getId(pmid, pmc))
                .pmid(pmid ? pmid.text() : null)
                .pmc(pmc ? pmc.text() : null)
                .mesh(mesh.join(" "))
                .pubmed()

        return document
    }

    String getId(Node pmid, Node pmc) {
        if (pmid) return pmid.text()
        if (pmc) return pmc.text()
        return UUID.randomUUID().toString()
    }

}

package org.lappsgrid.index.corpus.pubmed

import org.lappsgrid.index.api.Extractor
import org.lappsgrid.index.model.LappsDocument

import java.util.concurrent.atomic.AtomicInteger

/**
 *
 */
class PubmedExtractor implements Extractor {

    AtomicInteger counter

    PubmedExtractor(AtomicInteger counter) {
        this.counter = counter
    }

    LappsDocument extract(File file) {
        LappsDocument doc = extract(file.newInputStream())
        if (doc != null) {
            counter.incrementAndGet()
            doc.path(file.path)
        }
        return doc
    }

    LappsDocument extract(InputStream stream) {
        return extract(parser.parse(stream))
    }

    LappsDocument extract(Reader reader) {
        return extract(parser.parse(reader))
    }

    LappsDocument extract(Node pubmed) {
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

        String pmcid = (pmc ? pmc.text() : "unknown")
        LappsDocument document = new LappsDocument()
                .title(title)
                .theAbstract(articleAbstract)
                .journal(journal.Title.text())
                .year(year)
                .id(getId(pmid, pmc))
                .pmid(pmid ? pmid.text() : null)
                .pmc(pmc ? pmc.text() : null)
                .mesh(mesh.join(" "))
                .url("https://www.ncbi.nlm.nih.gov/pmc/articles/${pmcid}/?report=classic")
                .pubmed()

        return document
    }

    String getId(Node pmid, Node pmc) {
        if (pmid) return pmid.text()
        if (pmc) return pmc.text()
        return UUID.randomUUID().toString()
    }

}

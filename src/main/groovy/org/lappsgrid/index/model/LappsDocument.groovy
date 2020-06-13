package org.lappsgrid.index.model

import java.lang.reflect.Field

/**
 * Data model for documents inserted into Solr and ElasticSearch.
 */
class LappsDocument {

    public static final class Type {
        public static final String PMC = "pmc"
        public static final String PUBMED = "pubmed"
        public static final String RXIV = "rxiv"
    }

    Map<String,Object> values
    int size

    public LappsDocument() {
        values = [:]
        size = 0
    }

    public LappsDocument(Map map) {
        map.each { key,value ->
            if (key == 'body') {
                size = ((String)value).length()
            }
//            document.addField(key, value)
            add(key, value)
        }
    }

    Map<String,Object> fields() {
        return values
    }

    LappsDocument id(String id) {
        if (id == null || id == 'null') {
            id = UUID.randomUUID().toString()
        }
        add(Fields.ID, id)
    }

    LappsDocument pmid(String id) {
        add(Fields.PMID, id)
    }

    LappsDocument pmc(String id) {
        add(Fields.PMC, id)
    }

    LappsDocument doi(String id) {
        add(Fields.DOI, id)
    }

    LappsDocument title(String title) {
        add(Fields.TITLE, title)
    }

    LappsDocument year(int year) {
        add(Fields.YEAR, Integer.toString(year))
    }

    LappsDocument year(String year) {
        add(Fields.YEAR, year)
    }

    LappsDocument theAbstract(String theAbstract) {
        add(Fields.ABSTRACT, theAbstract)
    }
    LappsDocument articleAbstract(String articleAbstract) {
        add(Fields.ABSTRACT, articleAbstract)
    }

    LappsDocument author(String name) {
        add(Fields.AUTHOR, name)
    }

    LappsDocument authors(String names) {
        if (names == null) {
            return this
        }
        add(Fields.AUTHOR, names.tokenize(';'))
    }

    LappsDocument intro(String intro) {
        add(Fields.INTRO, intro)
    }
    LappsDocument introduction(String intro) {
        add(Fields.INTRO, intro)
    }

    LappsDocument discussion(String discussion) {
        add(Fields.DISCUSSION, discussion)
    }

    LappsDocument body(String body) {
        add(Fields.BODY, body)
    }

    LappsDocument results(String results) {
        add(Fields.RESULTS, results)
    }

    LappsDocument path(String path) {
        add(Fields.PATH, path)
    }

    LappsDocument mesh(String mesh) {
        add(Fields.MESH, mesh)
    }

    LappsDocument keywords(String keywords) {
        add(Fields.KEYWORDS, keywords)
    }

    LappsDocument journal(String journal) {
        add(Fields.JOURNAL, journal)
    }

    LappsDocument license(String license) {
        add(Fields.LICENSE, license)
    }

    LappsDocument url(String url) {
        add(Fields.URL, url)
    }

    LappsDocument type(String type) {
        add(Fields.TYPE, type)
    }

    LappsDocument pmc() {
        type(Type.PMC)
    }

    LappsDocument pubmed() {
        type(Type.PUBMED)
    }

    LappsDocument rxiv() {
        type(Type.RXIV)
    }

    String title() {
        return getValue(Fields.TITLE)
    }

    String articleAbstract() {
        return getValue(Fields.ABSTRACT)
    }

    String body() {
        return getValue(Fields.BODY)
    }

    String license() {
        return getValue(Fields.LICENSE)
    }

    String url() {
        return getValue(Fields.URL)
    }

    String getValue(String name) {
        return values.get(name).toString()
    }

    Collection<Object> getValues(String name) {
        return values.get(name)
    }

    private LappsDocument add(String name, Object value) {
        values.put(name, value)
        return this
    }
}

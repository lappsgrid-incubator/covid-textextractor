package org.lappsgrid.index.corpus.cord19

import org.lappsgrid.index.utils.CSVParser

//@Grab("com.opencsv:opencsv:5.1")
import com.opencsv.CSVReader

/**
 *
 */
class Metadata {

//    Index keys in the CSV are:
//    cord_uid
//    sha
//    source_x
//    title
//    doi
//    pmcid
//    pubmed_id
//    license
//    abstract
//    publish_time
//    authors
//    journal
//    Microsoft Academic Paper ID
//    WHO #Covidence
//    has_pdf_parse
//    has_pmc_xml_parse
//    full_text_file
//    url
    Map<String,Map> index = [:]
    Map<String,Map> sha  = [:]
    int duplicates

    Metadata load() {
        return load(this.class.getResourceAsStream("/metadata.csv"))
    }

    Metadata load(String path) {
        return load(new FileInputStream(path))
    }

    Metadata load(InputStream stream) {
        return load(stream.newReader())
    }

    Metadata load(File file) {
        return load(file.newReader())
    }

    Metadata load(Reader r) {
        CSVReader reader = new CSVReader(r)
//        CSVParser parser = new CSVParser()
//        List<String[]> list = parser.parse(reader)
//        Iterator<String> csv = list.iterator()
        String[] keys = reader.readNext()
        String[] line //= reader.readNext()

        int bad = 0
        int n = 0
        while ((line = reader.readNext()) != null) {
            ++n
            try {
                Map entry = [:]
                line.eachWithIndex{ String value, int i ->
                    entry.put(keys[i], value)
                }
                if (index.containsKey(entry.pmcid)) {
                    ++duplicates
                }
                else {
                    index.put(entry.pmcid, entry)
                    entry.sha.split(';').each { hash ->
                        sha[hash.trim()] = entry
                    }
                }
            }
            catch (Exception e) {
                ++bad
                println "Line $n is invalid: ${e.message}"
                line.eachWithIndex { String entry, int i ->
                    println "$i $entry"
                }
                throw e
            }
        }
        if (bad > 0) {
            println "The metadata file contains $bad invalid lines."
        }
//        if (dupes > 0) {
//            println "The metadata file contains $dupes duplicate entries"
//        }
        return this
    }

    List<Map> findAll(Closure query) {
        return index.findAll(query)
    }

    Map findBySha(String sha) {
        return this.sha[sha]
    }

    Map lookup(String id) {
        return index[id]
    }

    int size() { return index.size() }

    static void main(String[] args) {
        Metadata md = new Metadata()
        md.load()
        println "Loaded ${md.size()} entries."
    }
}

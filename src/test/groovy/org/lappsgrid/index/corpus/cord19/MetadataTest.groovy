package org.lappsgrid.index.corpus.cord19

import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.lappsgrid.index.utils.FileSystemIterator
import org.lappsgrid.index.utils.SimpleTimer
import org.lappsgrid.serialization.Serializer

/**
 *
 */
@Ignore
class MetadataTest {

    Metadata md

    @Before
    void setup() {
        File file = new File("/var/corpora/covid/2020-06-11/metadata.csv")
        assert file.exists()
        md = new Metadata().load(file)

    }

    @After
    void teardown() {
        md = null
    }

    @Test
    void getAuthors() {
//        Metadata md = new Metadata().load()
        Map entry = md.lookup("PMC4834006")
        println entry.authors
        println entry.publish_time
    }

    @Test
    void allDates() {
        Metadata md = new Metadata().load()
        Map<Integer, Integer> years = [] as HashMap
        md.index.each { k,v ->
            int year = v.publish_time.split('-')[0] as int
            Integer count = years[year]
            if (count == null) {
                count = 0
            }
            years[year] = count + 1
        }
        years.sort{a,b -> b.key <=> a.key}.each { k,v -> println "$k : $v" }
    }

    @Test
    void authors() {
        Metadata md = new Metadata().load()
        println "Size: ${md.size()}"
        List<Map> unauthored = []
        md.index.each { key, entry ->
            if (entry.authors == null || entry.authors.toString().length() == 0) {
                unauthored << entry
            }
        }
        println "There are ${unauthored.size()} papers with no listed authors"
        println Serializer.toPrettyJson(unauthored[0])
    }

    @Test
    void pdfParsed() {
        SimpleTimer totalTime = new SimpleTimer().start()
        SimpleTimer loadTime = new SimpleTimer().start()
        Metadata md = new Metadata().load("/var/corpora/covid/2020-06-11/metadata.csv")
        loadTime.stop()

        println md.index.size()
        int both, pdf, pmc, other
        both = pdf = pmc = other = 0
        String key = md.index.keySet().iterator().next()
        println Serializer.toPrettyJson(md.lookup(key))
        SimpleTimer iterationTime = new SimpleTimer().start()
        md.index.each { id, entry ->
            if (entry.pmc_json_files  && entry.pdf_json_files) {
                ++both
            }
            else if (entry.pmc_json_files) {
                ++pmc
            }
            else if (entry.pdf_json_files) {
                ++pdf
            }
            else {
                ++other
            }
        }
        iterationTime.stop()
        totalTime.stop()
        println "COUNTS"
        println "======"
        println "Both: $both"
        println "PDF : $pdf"
        println "PMC : $pmc"
        println "None: $other"
        println "Total: ${both + pmc + pdf + other}"
        println()
        println "TIMES"
        println "====="
        printf("Load     : %s\n", loadTime)
        printf("Iteration: %s\n", iterationTime)
        printf("Total    : %s\n", totalTime)

    }

    @Test
    void noSHA() {
        Metadata md = new Metadata().load()
//        println md.index.findAll { k,v ->
//            v.sha != null
//        }.each { k,v ->
//            println "${v.sha}" // ${v.full_text_file}"
//        }
        int found = 0
        int missing = 0
        md.index.each { k,v ->
            if (v.sha) {
                String[] shas = v.sha.split(';')
                shas.each { String sha ->
                    File f = findFind(sha, v.full_text_file)
                    if (f.exists()) {
                        ++found
                        println "$sha\t${f.path}"
                    }
                    else {
                        ++missing
                    }
                }
            }
        }
        println "Found: $found"
        println "Missing: $missing"
    }

    File findFind(String sha, String dirName) {
//        List<String> dirs = [ 'biorxiv_medrxiv', 'comm_use_subset', 'noncomm_use_subset', 'custom_license']
        File root = new File('/var/corpora/covid/' + dirName + '/pdf_json')
        if (!root.exists()) {
            throw new RuntimeException("Unable to find root directory. " + root.path)
        }
        return new File(root, sha + '.json')
    }

    @Test
    void scanFiles() {
        int missing = 0
        int found = 0
        Metadata md = new Metadata().load()
        List<String> dirs = [ 'biorxiv_medrxiv', 'comm_use_subset', 'noncomm_use_subset', 'custom_license']
        File root = new File("/var/corpora/covid/")
        dirs.each { String subdir ->
            File dir = new File(root, "$subdir/pdf_json")
            println "Scanning ${dir.path}"
            if (!dir.exists()) {
                throw new IOException("Unable to find dirctory " + dir.path)
            }
            FileFilter filter = { File f -> f.name.endsWith(".json")}
            dir.listFiles(filter).each { File f ->
                String sha = f.name.replace('.json', '')
                Map entry = md.findBySha(sha)
                if (entry == null) {
                    ++missing
                }
                else {
                    ++found
                    println "${entry.pmcid} ${entry.doi} ${entry.url}"
                }
            }
        }
        println "Found: $found"
        println "Missing: $missing"
    }

    final static List keys = ['paper_id', 'pmc_id', 'pubmed_id']
    @Test
    void lookForAllFiles() {
        Metadata metadata = new Metadata().load()
        File dir = new File("/var/corpora/covid")
        FileSystemIterator files = new FileSystemIterator(dir)
        File file = files.next()
        List<File> unknown = []
        int foundCount = 0
        int missingPMC = 0
        int missingPDF = 0
        int n = 0
        Set<String> seen = new HashSet<>()
        while (file != null) {
            if (seen.contains(file.path)) {
                throw new RuntimeException("We have already seen " + file.path)
            }
            seen.add(file.path)
            ++n
            printf("%05d %s\n", n, file.path)
            boolean found = false
            Map document = Serializer.parse(file.text, HashMap)
            for (String key : keys) {
                Map data = metadata.lookup(document[key])
                if (data != null) {
                    found = true
//                    break;
//                    println "Found metadata for $key ${file.path}"
                }
            }
            if (found) {
                ++foundCount
            }
            else {
                Map data = metadata.findBySha(document.paper_id)
                if (data == null) {
//                    throw new RuntimeException("No metadata for ${file.path}")
                    unknown.add(file)
                    if (file.path.contains("pdf_json")) {
                        ++missingPDF
                    }
                    else {
                        ++missingPMC
                    }
                }
                else {
                    ++foundCount
                }
            }
            file = files.next()
        }
        println "Found:          : $foundCount"
        println "Missing metadata: ${unknown.size()}"
        println "Missing PDF     : $missingPDF"
        println "Missing PMC     : $missingPMC"
    }
}

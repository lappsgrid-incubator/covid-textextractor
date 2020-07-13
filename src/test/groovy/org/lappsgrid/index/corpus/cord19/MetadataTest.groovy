package org.lappsgrid.index.corpus.cord19

import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.lappsgrid.index.api.FileIterator
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
        md.pmc.each { k, v ->
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
        md.pmc.each { key, entry ->
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

        println md.pmc.size()
        int both, pdf, pmc, other
        both = pdf = pmc = other = 0
        String key = md.pmc.keySet().iterator().next()
        println Serializer.toPrettyJson(md.lookup(key))
        SimpleTimer iterationTime = new SimpleTimer().start()
        md.pmc.each { id, entry ->
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
        println "Dupes: ${md.duplicates}"
        println "Total: ${both + pmc + pdf + other + md.duplicates}"
        println "Usable: ${both + pmc + pdf}"
        println()
        println "TIMES"
        println "====="
        printf("Load     : %s\n", loadTime)
        printf("Iteration: %s\n", iterationTime)
        printf("Total    : %s\n", totalTime)

    }

    @Test
    void noSHA() {
        Metadata md = new Metadata().load("/var/corpora/covid/2020-06-11/metadata.csv")
//        println md.index.findAll { k,v ->
//            v.sha != null
//        }.each { k,v ->
//            println "${v.sha}" // ${v.full_text_file}"
//        }
        File corpus = new File("/var/corpora/covid/2020-06-11/document_parses")
        int found = 0
        int missing = 0
        int noId = 0
        FileIterator files = new FileSystemIterator(corpus)
        File file = null
        while ((file = files.next()) != null) {
            printf "%06d %06d %06d %s\n", found, missing, noId, file.path
            Map document = Serializer.parse(file.text, HashMap)
            if (document.paper_id) {
                Map entry = find(document)
                if (entry) {
                    ++found
                }
                else {
                    ++missing
//                    println file.text
//                    throw new Exception("FF")
                }
            }
            else {
                ++noId
            }
        }
        println "Found: $found"
        println "Missing: $missing"
        println "No ID: $noId"
    }

    Map find(Map document) {
        String id = document.paper_id
        Map entry = md.lookup(id)
        if (entry) return entry
        return md.findBySha(id)
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
                    break;
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

    String vespa_result = '''
{
    "root": {
        "id": "toplevel",
        "relevance": 1.0,
        "fields": {
            "totalCount": 40
        },
        "coverage": {
            "coverage": 100,
            "documents": 127518,
            "full": true,
            "nodes": 2,
            "results": 1,
            "resultsFull": 1
        },
        "children": [
            {
                "id": "index:content/1/f891f215817911caf29baa10",
                "relevance": 0.3119366171951002,
                "source": "content",
                "fields": {
                    "id": 107041,
                    "doi": "https://doi.org/10.1016/j.scitotenv.2020.138862"
                }
            },
            {
                "id": "index:content/1/5adc90bb95aaaa719f55ec49",
                "relevance": 0.3110290215827607,
                "source": "content",
                "fields": {
                    "id": 59757,
                    "doi": "https://doi.org/10.1101/2020.03.02.20030148"
                }
            },
            {
                "id": "index:content/0/a365225968832f6c99867dab",
                "relevance": 0.29888931013857606,
                "source": "content",
                "fields": {
                    "id": 114569,
                    "doi": "https://doi.org/10.1128/jvi.02140-18"
                }
            },
            {
                "id": "index:content/0/7adaf6a36d64c611dcd6277b",
                "relevance": 0.2874200775557979,
                "source": "content",
                "fields": {
                    "id": 124418,
                    "doi": "https://doi.org/10.1016/j.scitotenv.2020.138872"
                }
            },
            {
                "id": "index:content/0/ac19070e8010ff7d9a238209",
                "relevance": 0.23762203125270026,
                "source": "content",
                "fields": {
                    "id": 106680,
                    "doi": "https://doi.org/10.1016/j.scitotenv.2020.138201"
                }
            },
            {
                "id": "index:content/1/3ae636e29882012b6d18489b",
                "relevance": 0.23465103563959636,
                "source": "content",
                "fields": {
                    "id": 97878,
                    "doi": "https://doi.org/10.1016/j.scitotenv.2020.139051"
                }
            },
            {
                "id": "index:content/0/44ff36795b550ce86c7b47f2",
                "relevance": 0.23439502471328863,
                "source": "content",
                "fields": {
                    "id": 59202,
                    "doi": "https://doi.org/10.1101/2020.04.07.029934"
                }
            },
            {
                "id": "index:content/0/c404faf601203dd219ebebbb",
                "relevance": 0.228358594783792,
                "source": "content",
                "fields": {
                    "id": 66372
                }
            },
            {
                "id": "index:content/0/a1a77df672a98c0b9027f101",
                "relevance": 0.2195196613476365,
                "source": "content",
                "fields": {
                    "id": 113208,
                    "doi": "https://doi.org/10.1016/j.tvjl.2007.03.019"
                }
            },
            {
                "id": "index:content/1/24b52a10ac7cdd18bf23cef6",
                "relevance": 0.2030461809784354,
                "source": "content",
                "fields": {
                    "id": 63079
                }
            },
            {
                "id": "index:content/1/162299485583e99ecfe61b2d",
                "relevance": 0.2021131812305787,
                "source": "content",
                "fields": {
                    "id": 72098
                }
            },
            {
                "id": "index:content/1/3acd445b6462194ee6e50190",
                "relevance": 0.20085004587506922,
                "source": "content",
                "fields": {
                    "id": 73172
                }
            },
            {
                "id": "index:content/1/2a57bbcdb22ad041205ed307",
                "relevance": 0.20037879472347397,
                "source": "content",
                "fields": {
                    "id": 82885
                }
            },
            {
                "id": "index:content/0/74208afa66dd64bbd98af3c6",
                "relevance": 0.19828251476836492,
                "source": "content",
                "fields": {
                    "id": 82135
                }
            },
            {
                "id": "index:content/1/e0a18cf9b1747fa92090d3dd",
                "relevance": 0.1929255323395855,
                "source": "content",
                "fields": {
                    "id": 75350
                }
            },
            {
                "id": "index:content/0/4a5cfa925b6c7e02631eac80",
                "relevance": 0.1928887824631957,
                "source": "content",
                "fields": {
                    "id": 6239,
                    "doi": "https://doi.org/10.1007/s007050170011"
                }
            },
            {
                "id": "index:content/1/1236cefa36221e09c4d0f7a0",
                "relevance": 0.1905603073358761,
                "source": "content",
                "fields": {
                    "id": 117424,
                    "doi": "https://doi.org/10.1021/acsnano.0c02439"
                }
            },
            {
                "id": "index:content/0/de0684da4331f37ebe22b77b",
                "relevance": 0.18814855043421394,
                "source": "content",
                "fields": {
                    "id": 83358
                }
            },
            {
                "id": "index:content/0/0284b0262497061389794f0f",
                "relevance": 0.186423104847259,
                "source": "content",
                "fields": {
                    "id": 91864,
                    "doi": "https://doi.org/10.1016/j.jpha.2020.02.010"
                }
            },
            {
                "id": "index:content/1/62237dc85dea92b801cafc54",
                "relevance": 0.18590983372259873,
                "source": "content",
                "fields": {
                    "id": 116719,
                    "doi": "https://doi.org/10.1186/1743-422x-11-139"
                }
            }
        ]
    }
}
'''
    @Test
    void parseResults() {
        Metadata md = new Metadata().load("/Users/suderman/Desktop/metadata-fixed.csv")

//        Map entry = md.lookup('32579059')
//        println entry == null ? 'not found' : 'found'
        Map map = Serializer.parse(vespa_result, HashMap)
        map.root.children.each { Map node ->
            if (node.fields.doi) {
                String doi = node.fields.doi.substring(16)
//                println doi
                Map entry = md.findByDoi(doi)
                if (entry != null) {
                    println "Found entry for $doi"
                }
                else {
                    println "No entry for $doi"
                }
            }
        }
    }

    @Test
    void doiTest() {
//        Metadata md = new Metadata().load("/var/corpora/covid/2020-06-15/metadata-fixed.csv")
        Metadata md = new Metadata().load("/Users/suderman/Desktop/metadata-fixed.csv")
//        Map entry = md.findByDoi('10.1080/07391102.2020.1782265')
        Map entry = md.lookup('32375574')
        if (entry == null) {
            println "Not found."
        }
        else {
            println Serializer.toPrettyJson(entry)
        }
//        entry = md.findBySha('5adc90bb95aaaa719f55ec49')
//        if (entry == null) {
//            println "Not found."
//        }
//        else {
//            println Serializer.toPrettyJson(entry)
//        }
//        String key = md.index.keySet().iterator().next()
//        entry = md.index[key]
//        println Serializer.toPrettyJson(entry)
    }


    @Test
    void prettyPrint() {
        printf "%4.1fMB\n", Runtime.getRuntime().totalMemory()/(1024*1024)
        File file = new File("/Users/suderman/Downloads/litcovid2pubtator.json")
        if (!file.exists()) {
            println "Input file not found"
            return
        }
        println "Parsing"
        List data = Serializer.parse(file.text, List)

        println Serializer.toPrettyJson(data)
    }

    @Test
    void groovyPrettyPrint() {
        printf "%4.1fMB\n", Runtime.getRuntime().totalMemory()/(1024*1024)
        File file = new File("/Users/suderman/Downloads/litcovid2pubtator.json")
        if (!file.exists()) {
            println "Input file not found"
            return
        }
        println "Parsing"
        println groovy.json.JsonOutput.prettyPrint(file.text)
    }


    @Test
    void cordLitCovidOverlap() {
        println "Loading metadata"
        Metadata md = new Metadata().load("/var/corpora/covid/2020-06-28/metadata-fixed.csv")

        Map e = md.lookup("PMC7118659")
        if (e == null) throw NoSuchElementException("No element with ID PMC7118659")
        File file = new File("/Users/suderman/Downloads/litcovid2pubtator.json")
        if (!file.exists()) {
            println "Input file not found"
            return
        }

//        println groovy.json.JsonOutput.prettyPrint(file.text)
        println "Parsing"
        Set<String> found = new HashSet<>()
        List list = Serializer.parse(file.text, List)
        List items = list[1]
        println "There are ${items.size()} articles in LitCovid"
        int notfound = 0
        items.each { Map item ->
            if (item._id == "32171740|PMC7118659") {
                println "Found ${item._id}"
            }
            String id = getId(item._id)
            if ("PMC7118659" == id || "32171740" == id) { println("Found it")}
            Map entry = md.lookup(id)
            if (entry) {
                if (entry.pmcid) found.add(entry.pmcid)
                else found.add(id)
//                found.add(id)
            }
            else {
                ++notfound
            }
        }
        println "Found in both : ${found.size()}"
        println "LitCovid only : $notfound"
        String json = Serializer.toPrettyJson(found)
        println json
        new File("/tmp/cord-litcovid-overlap.json").text = json
    }

    String getId(String input) {
        String[] parts = input.split("\\|")
        if (parts[1] == "None") {
            return parts[0]
        }
        return parts[1]
    }

    @Test
    void wtf() {
        def parts = "32525830|None".split("\\|")
        assert 2 == parts.size()
    }
}

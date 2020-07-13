package org.lappsgrid.index.corpus.cord19

/**
 *
 */
class MetadataLookup {

    void run() {
        println "Loading metadata"
        Metadata md = new Metadata().load("/var/corpora/covid/2020-06-28/metadata-fixed.csv")
        Scanner scanner = new Scanner(System.in)
        boolean running = true
        while (running) {
            print "enter ID: "
            String input = scanner.next()
            if (input == "exit" || input == "quit") {
                running = false
            }
            else {
                Map entry = md.lookup(input)
                if (entry) {
                    //println entry.pmc_json_files ?: entry.pdf_json_files ?: "no such file"
                    println entry.title
                }
                else {
                    println "Not found."
                }
            }
        }
    }

    static void main(String[] args) {
        new MetadataLookup().run()
    }
}

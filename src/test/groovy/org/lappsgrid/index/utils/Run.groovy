package org.lappsgrid.index.utils

import org.junit.Ignore
import org.junit.Test
import org.lappsgrid.serialization.Serializer
import org.lappsgrid.index.cli.Application

/**
 *
 */
@Ignore
class Run {

    @Test
    void run() {
        Application.main('--cord19 --limit 10 -i /var/corpora/covid/noncomm_use_subset/pmc_json/ -t 4 -c covid19'.split(' '))
    }

    @Test
    void help() {
        Application.main("--help")
    }

    @Test
    void version() {
        Application.main("--version")
    }

    @Test
    void parseAll() {
        File dir = new File("/var/corpora/covid/noncomm_use_subset/pmc_json")
        for (File file : dir.listFiles()) {
            String text = parse(file)
            printf("%s\t%d\n", file.name, text.length())
        }
    }

    void bodyText() {
        String name = 'PMC1616946.xml.json'
        File dir = new File("/var/corpora/covid/noncomm_use_subset/pmc_json")
        File file = new File(dir, name)


    }

    String parse(File file) {
        StringWriter writer = new StringWriter()
        PrintWriter out = new PrintWriter(writer)
        Map doc = Serializer.parse(file.text, HashMap)
        doc.body_text.each {
            out.println it.section
            out.println it.text
            out.println()
        }
        return writer.toString()
    }

    @Test
    void es() {
        Application.main("--input /var/corpora/covid/noncomm_use_subset/pmc_json --cord19 --core cord19-2020-04-26 --threads 6 --batch-size 100 --elastic http://localhost:9200".split(" "))
    }


}

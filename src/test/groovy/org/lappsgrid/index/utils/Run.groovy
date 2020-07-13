package org.lappsgrid.index.utils

import org.junit.Ignore
import org.junit.Test
import org.lappsgrid.serialization.Serializer
import org.lappsgrid.index.cli.Application

import java.text.DateFormat
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle

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
        println DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG).format(ZonedDateTime.now())
        println DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG).format(ZonedDateTime.now())
//        println DateTimeFormatter.ISO_ZONED_DATE_TIME.format(LocalTime.now());
//        Date now = new Date()
//        printf("%4d-%02d-%02d %02d:%02d:%02d %d\n", now.year+1900, now.month+1, now.date, now.hours, now.minutes, now.seconds, now.timezoneOffset/60)
    }


}

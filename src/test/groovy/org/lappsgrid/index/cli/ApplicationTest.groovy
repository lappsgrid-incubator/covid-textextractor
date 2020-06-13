package org.lappsgrid.index.cli


import org.junit.Test

/**
 *
 */
class ApplicationTest {

    @Test
    void printHelp() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream()
        ByteArrayOutputStream errors = new ByteArrayOutputStream()
        PrintStream stdout = System.out
        PrintStream stderr = System.err
        System.setOut(new PrintStream(bytes))
        System.setErr(stderr)
        Application.main("help")
        System.setOut(stdout)
        System.setErr(stderr)

        assert 0 == errors.size()
        println bytes.toString()
    }

    @Test
    void pubmedHelp() {
        Application.main("help pubmed".split(' '))
    }

//    @Test
//    void pubmed() {
//        Application.main("-i /tmp -t 5 pubmed -s 1 -e 5".split(' '))
//    }

}

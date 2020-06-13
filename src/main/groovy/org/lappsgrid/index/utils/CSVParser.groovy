package org.lappsgrid.index.utils

import java.util.function.Consumer

/**
 *
 */
class CSVParser {

    enum State {
        start, in_field, single_quote, end_quote, skip
    }

    CSVParser() {

    }

    List<String[]> parse(File file) {
        return parse(file.newInputStream())
    }

    List<String[]> parse(InputStream stream) {
        List<String[]> result = []
        stream.eachLine {
            result.add(parseLine(it))
        }
        return result
    }

    List<String[]> parse(Reader reader) {
        List<String[]> result = []
        reader.eachLine {
            result.add(parseLine(it))
        }
        return result
    }

    private String[] parseLine(String line) {
//        println "Parsing line: ${line.length()}"
        char[] chars = line.chars
        List<String> fields = []
        StringBuilder buffer = new StringBuilder()
        State state = State.start
        State previous = State.start
        for (int i = 0; i < chars.length; ++i) {
            char ch = chars[i]
            if (state == State.start) {
                if (ch == '"') {
                    state = State.single_quote
                }
                else {
                    state = State.in_field
                    buffer.append(ch)
                }
            }
            else if (state == State.skip) {
                // We are skipping a second double quote.
                state = previous
                previous = State.start
            }
            else if (state == State.in_field) {
                if (ch == ',') {
                    fields.add(buffer.toString())
                    buffer = new StringBuilder()
                    state = State.start
                }
                else if (ch == '"') {
                    if (chars[i+1] == '"') {
                        buffer.append('"')
                        previous = state
                        state = State.skip
                    }
                    else {
                        state = State.end_quote
                    }
                }
                else {
                    buffer.append(ch)
                }
            }
            else if (state == State.single_quote) {
                if (ch == '"') {
                    if (i+1<chars.length && chars[i+1] == '"') {
                        buffer.append('"')
                        previous = state
                        state = State.skip
                    }
                    else {
//                        buffer.append(ch)
                        state = State.end_quote
                    }
                }
                else {
                    buffer.append(ch)
                }
            }
            else if ( state == State.end_quote) {
                if (ch == ',') {
                    fields.add(buffer.toString())
                    buffer = new StringBuilder()
                    state = State.start
                }
                else {
                    //TODO Should we throw here?  It loosk like ...,"some quoted string" more text,...
                    buffer.append(ch)
                    state = State.in_field
                }
            }
            else {
                throw new Exception("Invalid state $state")
            }
        }
        return fields.toArray()
    }
}

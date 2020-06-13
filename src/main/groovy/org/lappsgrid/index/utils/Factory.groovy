package org.lappsgrid.index.utils

/**
 * Class used to create other objects, in particular XML parsers since the default parsers
 * are insecure.
 */
class Factory {
    static XmlParser createXmlParser() {
        XmlParser parser = new XmlParser();
        parser.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false)
        parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
        return parser
    }

    static XmlSlurper createXmlSlurper() {
        XmlSlurper parser = new XmlSlurper();
        parser.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false)
        parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
        return parser
    }
}

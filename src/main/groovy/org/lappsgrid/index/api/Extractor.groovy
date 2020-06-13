package org.lappsgrid.index.api

import org.lappsgrid.index.model.LappsDocument

/**
 *
 */
interface Extractor {
    LappsDocument extract(File file)
}
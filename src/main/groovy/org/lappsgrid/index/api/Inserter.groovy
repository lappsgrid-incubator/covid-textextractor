package org.lappsgrid.index.api

import org.lappsgrid.index.model.LappsDocument

/**
 *
 */
interface Inserter {
    boolean insert(LappsDocument document)
    void commit()
}
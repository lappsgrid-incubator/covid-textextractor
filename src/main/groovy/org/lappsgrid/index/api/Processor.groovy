package org.lappsgrid.index.api

import org.lappsgrid.index.model.LappsDocument

/**
 *
 */
interface Processor {
    LappsDocument process(File file)
}
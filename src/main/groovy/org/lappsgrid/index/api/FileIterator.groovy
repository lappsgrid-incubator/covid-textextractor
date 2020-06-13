package org.lappsgrid.index.api

/**
 * Iterates over a collection of files.
 */
interface FileIterator {
    /**
     * Get the next file.
     *
     * @return the next File in the sequence or NULL if the end of the sequence has been
     * reached.
     */
    File next();
}
package org.lappsgrid.index.utils

import org.lappsgrid.index.api.FileIterator

import java.util.concurrent.locks.ReentrantLock

/**
 * Generate a sequences
 */
class FileSystemIterator implements FileIterator {

    static final FileFilter ALL = { f -> true }
    static final FileFilter JSON = { File f -> f.isDirectory() || f.name.endsWith(".json")}
    static final FileFilter TXT = { File f -> f.isDirectory() || f.name.endsWith(".txt")}
    static final FileFilter XML = { File f -> f.isDirectory() || f.name.endsWith(".xml")}

    /** The current list of files we are iterating through. */
    ArrayList<File> files
    /** Filter used to select files for iteration. */
    FileFilter filter
    /** Limit the number of files to be iterated over. A limit of -1 means
     *  iterate over all files. */
    int limit
    /** Count the number of files iterated over. */
    int count

    ReentrantLock lock

    FileSystemIterator(File directory) {
        this(directory, JSON, -1)
    }

    FileSystemIterator(File directory, int limit) {
        this(directory, JSON, limit)
    }

    FileSystemIterator(File directory, FileFilter filter) {
        this(directory, filter, -1)
    }

    FileSystemIterator(File directory, FileFilter filter, int limit) {
        files = Arrays.asList(directory.listFiles(filter)) //.asSynchronized()
        this.filter = filter
        this.limit = limit
        this.lock = new ReentrantLock()
    }

    File next() {
        lock.lock()
        if (files.size() == 0 || limit == 0) {
            // We are done.
            lock.unlock()
            return null
        }
        if (limit > 0 && count >= limit) {
            // We have reached the user defined limit on the number of files returned.
            lock.unlock()
            return null
        }
        // File.listFiles() might throw
        try {
            ++count
            File file = files.remove(0)
            while (file != null && file.isDirectory()) {
                File[] listing = file.listFiles(filter)
                files.addAll(listing)
                file = files.remove(0)
            }
            return file
        }
        finally {
            lock.unlock()
        }
    }
}

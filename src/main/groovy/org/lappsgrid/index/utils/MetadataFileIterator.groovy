package org.lappsgrid.index.utils

import org.lappsgrid.index.api.FileIterator
import org.lappsgrid.index.corpus.cord19.Metadata

/**
 *
 */
class MetadataFileIterator implements FileIterator {

    final int limit
    final File base
    final Iterator<Map.Entry<String,Map>> iterator
    int counter
    Metadata metadata

    MetadataFileIterator(String path, int limit = -1) {
        this(new File(path), limit);
    }

    MetadataFileIterator(File file, int limit = -1) {
        this(file.parentFile, new Metadata().load(file), limit)
    }

    MetadataFileIterator(File base, InputStream stream, int limit) {
        this(base, new Metadata().load(), limit)
    }

    MetadataFileIterator(File base, Metadata md, final int limit) {
        this.base = base
        this.limit = limit
        this.metadata = md
        iterator = md.pmc.iterator()
        counter = 0
    }

    synchronized File next() {
        if (limit > 0 && counter >= limit) {
            return null
        }
        ++counter
        while (iterator.hasNext()) {
            File candidate = null
            Map<String,String> entry = iterator.next().value;
            if (entry.pmc_json_files) {
                String value = entry.pmc_json_files
                value.split(";")
                candidate = getFile(base, entry.pmc_json_files)
            }
            else if (entry.pdf_json_files) {
                candidate = getFile(base, entry.pdf_json_files)
            }
            if (candidate) {
                if (candidate.exists()) {
                    return candidate
                }
                println "Entry ${entry.id} not found: ${candidate.path}"
            }
        }
        return null
    }

    File getFile(File base, String path) {
        if (path.contains(";")) {
            String[] parts = path.split(";")
            parts.each { String part ->
                File file = new File(base, part.trim())
                if (file.exists()) return file
            }
            return null
        }
        return new File(base, path)
    }
}

package org.lappsgrid.index.utils

import org.junit.Test

/**
 *
 */
class FileIteratorTest {

    @Test
    void filterAll() {
        FileFilter filter = FileSystemIterator.ALL
        ['foo.txt', 'foo.json', 'foo.xml', 'foo'].each { String name ->
            assert filter.accept(new File(name))
        }
    }
    @Test
    void filterJson() {
        FileFilter filter = FileSystemIterator.JSON
        ['foo.txt', 'foo.json', 'foo.xml', 'foo'].each { String name ->
            assert filter.accept(new File(name)) == name.endsWith('.json')
        }

    }

    @Test
    void filterText() {
        FileFilter filter = FileSystemIterator.TXT
        ['foo.txt', 'foo.json', 'foo.xml', 'foo'].each { String name ->
            assert filter.accept(new File(name)) == name.endsWith('.txt')
        }
    }

    @Test
    void filterXml() {
        FileFilter filter = FileSystemIterator.XML
        ['foo.txt', 'foo.json', 'foo.xml', 'foo'].each { String name ->
            assert filter.accept(new File(name)) == name.endsWith('.xml')
        }
    }

    @Test
    void singleDirectory() {
        String ext = '.json'
        File work = new File('/tmp', 'file-iterator')
        File dir = createTestDirectory(work,10) { File dir, int i ->
            touch(new File(dir, "file${i}${ext}"))
        }
        FileSystemIterator fit = new FileSystemIterator(dir)
        test(fit, 10)

        // Test limits
        fit = new FileSystemIterator(dir, 5)
        test(fit, 5)
        work.deleteDir()
    }

    @Test
    void nestedDirectories() {
        File tmp = new File("/tmp/")
        File work = new File(tmp, 'file-iterator')
        assert !work.exists()

        File json = createTestDirectory(work, 'json', 10) { File dir, int i ->
            touch(new File(dir, "json-${i}.json"))
        }
        File txt = createTestDirectory(work, 'txt', 5) { File dir, int i ->
            touch(new File (dir, "text-${i}.txt"))
        }
        File mixed = createTestDirectory(work, 'mixed', 5) { File dir, int i ->
            touch(new File(dir, "mixed-${i}.txt"))
            touch(new File(dir, "mixed-${i}.json"))
        }

        println "testing json"
        test(new FileSystemIterator(work, FileSystemIterator.JSON), 15)
        println "testing all"
        test(new FileSystemIterator(work, FileSystemIterator.ALL), 25)
        println "testing txt"
        test(new FileSystemIterator(work, FileSystemIterator.TXT), 10)
        work.deleteDir()
        assert !work.exists()
    }

    void touch(File file) {
       if (!file.exists()) {
           file.createNewFile()
           file.deleteOnExit()
       }
    }
    void test(FileSystemIterator files, int n) {
        n.times { int i ->
            File file = files.next()
            println "$i ${file.path}"
            assert file.exists()
        }
        assert files.next() == null
    }

    void delete(File directory) {
        for (File entry : directory.listFiles()) {
            if (entry.isDirectory()) {
                delete(entry)

            }
            if (!entry.delete()) {
                throw new IOException("Unable to delete ${entry.path}")
            }
            println "Deleted ${entry.path}"
        }
        directory.deleteDir()
    }

    File createTestDirectory(int nFiles, Closure cl) {
        return createTestDirectory(new File('/tmp'), nFiles, cl)
    }

    File createTestDirectory(File dir, String name, int nFiles, Closure createFile) {
        return createTestDirectory(new File(dir, name), nFiles, createFile)
    }

    File createTestDirectory(File dir, int nFiles, Closure createFile) {
//        File work = new File(base, UUID.randomUUID().toString())
        dir.mkdirs()
        assert dir.exists()
        nFiles.times { i -> createFile(dir, i) }
        dir.deleteOnExit()
        return dir
    }
}

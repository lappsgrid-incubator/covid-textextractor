package org.lappsgrid.index.cli

import org.lappsgrid.index.Version
import picocli.CommandLine

/**
 *
 */
class VersionProvider implements CommandLine.IVersionProvider {
    @Override
    String[] getVersion() throws Exception {
        return  [
                "",
                "LappsGrid Indexer v" + Version.version,
                "Copyright 2020 The Lanugage Applications Grid",
                ""
        ] as String[]
    }
}
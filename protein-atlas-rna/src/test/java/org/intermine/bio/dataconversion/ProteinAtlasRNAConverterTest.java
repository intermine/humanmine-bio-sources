package org.intermine.bio.dataconversion;

/*
 * Copyright (C) 2002-2017 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;

import org.intermine.dataconversion.ItemsTestCase;
import org.intermine.dataconversion.MockItemWriter;
import org.intermine.metadata.Model;
import org.intermine.model.fulldata.Item;
import org.intermine.xml.full.FullParser;

public class ProteinAtlasRNAConverterTest extends ItemsTestCase
{
    private final String fileName = "rna_tissue.tsv";

    public ProteinAtlasRNAConverterTest(String arg) {
        super(arg);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void testProcess() throws Exception {
        MockItemWriter itemWriter = new MockItemWriter(new HashMap<String, Item>());
        BioFileConverter converter = new ProteinAtlasRNAConverter(itemWriter,
                                                                   Model.getInstanceByName("genomic"));

        File testFile = new File(getClass().getClassLoader()
                .getResource(fileName).toURI());

        Reader reader = new InputStreamReader(getClass().getClassLoader()
                .getResourceAsStream(fileName));

        converter.setCurrentFile(testFile);
        converter.process(reader);
        converter.close();

        // uncomment to write out a new target items file
        writeItemsFile(itemWriter.getItems(), "protein-atlas-rna-tgt.xml");

        assertEquals(readItemSet("ProteinAtlasRNAConverterTest-tgt.xml"), itemWriter.getItems());
    }

    @SuppressWarnings("rawtypes")
    protected Collection getExpectedItems() throws Exception {
        return FullParser.parse(getClass().getClassLoader().getResourceAsStream("ProteinAtlasConverterTest.xml"));
    }

    // we don't parse the XML any more.
//    public void testXMLParsing() throws Exception {
//        MockItemWriter itemWriter = new MockItemWriter(new HashMap<String, Item>());
//        BioFileConverter converter = new ProteinAtlasConverter(itemWriter,
//                                                                   Model.getInstanceByName("genomic"));
//
//        File testFile = new File(getClass().getClassLoader()
//                .getResource("proteinatlas.xml").toURI());
//        converter.setCurrentFile(testFile);
//        converter.process(null);
//        converter.close();
//
//        // uncomment to write out a new target items file
//        // writeItemsFile(itemWriter.getItems(), "protein-atlas-tgt.xml");
//
//        assertEquals(readItemSet("ProteinAtlasConverterXMLParsingTest.xml"), itemWriter.getItems());
//    }
}
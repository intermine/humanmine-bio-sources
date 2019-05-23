package org.intermine.bio.dataconversion;

/*
 * Copyright (C) 2002-2018 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import org.intermine.dataconversion.ItemsTestCase;
import org.intermine.dataconversion.MockItemWriter;
import org.intermine.metadata.Model;
import org.intermine.xml.full.Item;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

public class AllenBrainExpressionConverterTest extends ItemsTestCase
{
    Model model = Model.getInstanceByName("genomic");
    AllenBrainExpressionConverter converter;
    MockItemWriter itemWriter;
    private Set<Item> storedItems;

    public AllenBrainExpressionConverterTest(String arg) {
        super(arg);
    }

    public void setUp() throws Exception {
        itemWriter = new MockItemWriter(new HashMap<String, org.intermine.model.fulldata.Item>());
        converter = new AllenBrainExpressionConverter(itemWriter, model);
        converter.rslv = IdResolverService.getMockIdResolver("Gene");
        converter.rslv.addResolverEntry("9606", "733", Collections.singleton("C8G"));
        converter.rslv.addResolverEntry("9606", "735", Collections.singleton("C9"));
        super.setUp();
    }

    public void testProcess() throws Exception {
        File tmp = new File(getClass().getClassLoader()
                .getResource("AllenBrainConverterTest_tgt.xml").toURI());
        File datadir = tmp.getParentFile();
        converter.process(datadir);
        converter.close();
        storedItems = itemWriter.getItems();
        writeItemsFile(storedItems, "allen-brain-expression-tgt-items.xml");

        Set<Item> expected = readItemSet("AllenBrainConverterTest_tgt.xml");
        assertEquals(expected, storedItems);
    }
}

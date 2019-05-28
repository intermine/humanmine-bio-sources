package org.intermine.bio.dataconversion;

/*
 * Copyright (C) 2002-2019 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;


/**
 * @author Julie Sullivan
 */
public class AllenBrainOntologyConverter extends BioFileConverter
{
    //
    private static final String DATASET_TITLE = "Allen Brain Structure Ontology";
    private static final String DATA_SOURCE_NAME = "Allen Brain Atlas";
    private Map<String, String> terms = new HashMap<String, String>();

    /**
     * Constructor
     * @param writer the ItemWriter used to handle the resultant items
     * @param model the Model
     */
    public AllenBrainOntologyConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
    }

    /**
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {
        Iterator<String[]> lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);
        // skip header
        lineIter.next();
        while (lineIter.hasNext()) {
            String[] line = lineIter.next();

            String identifier = line[0];
            String acronym = line[1];
            String name = line[2];
            String parents = line[6];
            storeTerm(String.valueOf(identifier), acronym, name, parents);
        }
    }

    private void storeTerm(String identifier, String acronym, String name, String structureIdPath)
        throws ObjectStoreException {
        Item item = createItem("BrainStructureTerm");
        item.setAttribute("identifier", identifier);
        item.setAttribute("acronym", acronym);
        item.setAttribute("name", name);
        if (structureIdPath != null) {
            String[] parents = structureIdPath.split("/");
            for (int i = 0; i < parents.length; i++) {
                String parentRefId = terms.get(parents[i]);
                if (parentRefId != null) {
                    item.addToCollection("parents", parentRefId);
                }
            }
        }
        store(item);
        terms.put(identifier, item.getIdentifier());
    }
}

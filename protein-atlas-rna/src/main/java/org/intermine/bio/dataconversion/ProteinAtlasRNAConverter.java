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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Read Protein Atlas RNA expression data.
 * @author Julie Sullivan
 */
public class ProteinAtlasRNAConverter extends BioFileConverter
{
    private static final Logger LOG = Logger.getLogger(ProteinAtlasRNAConverter.class);
    private static final String DATASET_TITLE = "Protein Atlas RNA Gene data";
    private static final String DATA_SOURCE_NAME = "The Human Protein Atlas";
    private Map<String, Item> genes = new HashMap<String, Item>();
    private Map<String, Item> tissues = new HashMap<String, Item>();
    private Set<String> storedTissues = new HashSet<String>();
    private int entryCount = 0;
    protected IdResolver rslv;
    private static final String TAXON_ID = "9606";

    /**
     * Constructor
     * @param writer the ItemWriter used to handle the resultant items
     * @param model the Model
     */
    public ProteinAtlasRNAConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
        if (rslv == null) {
            rslv = IdResolverService.getIdResolverByOrganism(Collections.singleton(TAXON_ID));
        }
    }

    /**
     * Read Protein Atlas normal_tissue.csv file.
     *
     * {@inheritDoc}
     */
    @Override
    public void process(Reader reader) throws Exception {
        File currentFile = getCurrentFile();
        if ("rna_tissue.tsv".equals(currentFile.getName())) {
            processTissue(reader);
        } else {
            throw new RuntimeException("Don't know how to process file: " + currentFile.getName());
        }
    }

    private void  processTissue(Reader reader) throws ObjectStoreException, IOException {
        // file has 5 colums:
        // Gene    Gene name       Sample  Value   Unit

        Iterator<String[]> lineIter = FormattedTextParser.parseTabDelimitedReader(reader);

        // skip header
        lineIter.next();

        while (lineIter.hasNext()) {
            String[] line = lineIter.next();

            String geneIdentifier = line[0];
            Item gene = getGene(geneIdentifier);
            if (gene == null) {
                continue;
            }

            String tissue = line[2];
            String expressionScore = line[3];

            Item item = createItem("RNASeqResult");
            item.setReference("gene", gene);
            item.setAttribute("tissue", tissue);
            item.setAttribute("expressionType", "TPM");
            // TODO if there isn't an expression score, maybe skip?
            if (StringUtils.isNotEmpty(expressionScore)) {
                item.setAttribute("expressionScore", expressionScore);
            }
            store(item);
            gene.addToCollection("rnaSeqResults", item);
        }
    }

    // store tells us we have been called with the upper case name from the tissue_to_organ file
    private Item getTissue(String tissueName) {
        Item tissue = tissues.get(tissueName);
        if (tissue == null) {
            tissue = createItem("Tissue");
            tissues.put(tissueName, tissue);
        }
        return tissue;
    }

    private Item getGene(String primaryIdentifier) throws ObjectStoreException {
        String resolvedIdentifier = resolveGene(primaryIdentifier);
        if (StringUtils.isEmpty(resolvedIdentifier)) {
            return null;
        }
        Item gene = genes.get(resolvedIdentifier);
        if (gene == null) {
            gene = createItem("Gene");
            gene.setAttribute("primaryIdentifier", resolvedIdentifier);
            gene.setReference("organism", getOrganism(TAXON_ID));
            store(gene);
            genes.put(resolvedIdentifier, gene);
        }
        return gene;
    }

    private String resolveGene(String identifier) {
        String id = identifier;
        if (rslv != null && rslv.hasTaxon(TAXON_ID)) {
            int resCount = rslv.countResolutions(TAXON_ID, identifier);
            if (resCount != 1) {
                LOG.info("RESOLVER: failed to resolve gene to one identifier, ignoring gene: "
                         + identifier + " count: " + resCount + " Human identifier: "
                         + rslv.resolveId(TAXON_ID, identifier));
                return null;
            }
            id = rslv.resolveId(TAXON_ID, identifier).iterator().next();
        }
        return id;
    }
}

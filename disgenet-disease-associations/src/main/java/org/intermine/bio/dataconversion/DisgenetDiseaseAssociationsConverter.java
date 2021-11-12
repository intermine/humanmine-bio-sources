package org.intermine.bio.dataconversion;

/*
 * Copyright (C) 2002-2020 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.commons.lang.StringUtils;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;


/**
 * @author Adrian Rodriguez-Bazaga (ar989@cam.ac.uk)
 */
public class DisgenetDiseaseAssociationsConverter extends BioDirectoryConverter
{
    private static final String DATASET_TITLE = "disgenet-disease-associations";
    private static final String DATA_SOURCE_NAME = "disgenet-disease-associations";

    private static final String TAXON_ID = "9606"; // Human Taxon ID
    private static final String DISGENET_FILE = "curated_gene_disease_associations.tsv";

    private Map<String, String> genes = new HashMap<String, String>();
    private Map<String, Item> diseases = new HashMap<String, Item>();

    protected IdResolver rslv;
    private static final Logger LOG = Logger.getLogger(DisgenetDiseaseAssociationsConverter.class);

    private String organismIdentifier; // objectid, not the taxon ID.

    /**
     * Constructor
     *
     * @param writer the ItemWriter used to handle the resultant items
     * @param model  the Model
     */
    public DisgenetDiseaseAssociationsConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
        if (rslv == null) {
            rslv = IdResolverService.getIdResolverByOrganism(TAXON_ID);
        }
    }


    /**
     *  Process
     *
     * @param dataDir  the directory with the data file(s)
     * @throws Exception exception!
     */

    public void process(File dataDir) throws Exception {

        Map<String, File> files = readFilesInDir(dataDir);
        organismIdentifier = getOrganism(TAXON_ID);
        processAssociations(new FileReader(files.get(DISGENET_FILE)));

    }

    private Map<String, File> readFilesInDir(File dir) {
        Map<String, File> files = new HashMap<String, File>();
        for (File file : dir.listFiles()) {
            files.put(file.getName(), file);
        }
        return files;
    }

    private void processAssociations(Reader reader)
            throws ObjectStoreException, IOException {
        Iterator<?> lineIter = FormattedTextParser.parseTabDelimitedReader(reader);
        // Skip header
        lineIter.next();

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();

            String geneNcbiId = line[0].trim();
            String geneSymbol = line[1];

            String diseaseId = line[4];
            String diseaseName = line[5];
            String diseaseType = line[6];
            String score = line[9];

            //Item disease = getDisease(diseaseId);
            Item disease = null;

            if (diseases.get(diseaseId) == null) {
                disease = createItem("Disease");
                disease.setAttribute("primaryIdentifier", diseaseId);
                disease.setAttribute("diseaseId", diseaseId);
                disease.setAttribute("name", diseaseName);
                disease.setAttribute("diseaseType", diseaseType);
                try {
                    store(disease);
                } catch (ObjectStoreException e) {
                    throw new RuntimeException("failed to store disease: " + diseaseId, e);
                }
                diseases.put(diseaseId, disease);

            } else {
                disease = getDisease(diseaseId);
            }

            String geneId = getGeneId(geneNcbiId, geneSymbol);

            if (StringUtils.isEmpty(geneId)) {
                continue;
            }

            Item interactionItem;
            interactionItem = createItem("DiseaseAssociation");
            interactionItem.setReference("gene", geneId);
            interactionItem.setReference("disease", disease);
            interactionItem.setAttribute("associationScore", score);
            try {
                store(interactionItem);
            } catch (ObjectStoreException e) {
                throw new RuntimeException("failed to store disease association: " + diseaseId, e);
            }
        }
    }


    private String getGeneId(String primaryIdentifier, String symbol) throws ObjectStoreException {
        String geneId = genes.get(primaryIdentifier);
        if (geneId == null) {
            Item gene = createItem("Gene");
            gene.setAttribute("primaryIdentifier", primaryIdentifier);
            gene.setAttribute("symbol", symbol);
            gene.setReference("organism", getOrganism(TAXON_ID));
            store(gene);
            geneId = gene.getIdentifier();
            genes.put(primaryIdentifier, geneId);
        }
        return geneId;
    }

    private Item getDisease(String diseaseId) {
        Item disease = diseases.get(diseaseId);
        return disease;
    }

    // -- NOT USED
    private String getGeneId(String primaryIdentifier) throws ObjectStoreException {
        String resolvedIdentifier = resolveGene(primaryIdentifier);
        if (StringUtils.isEmpty(resolvedIdentifier)) {
            return null;
        }
        String geneId = genes.get(primaryIdentifier);
        if (geneId == null) {
            Item gene = createItem("Gene");
            gene.setAttribute("symbol", primaryIdentifier);
            //gene.setAttribute("symbol", primaryIdentifier);
            //gene.setReference("organism", getOrganism(TAXON_ID));
            store(gene);
            geneId = gene.getIdentifier();
            genes.put(primaryIdentifier, geneId);
        }
        return geneId;
    }

    // -- NOT USED
    private String getGeneIdentifier(String geneSymbol) throws ObjectStoreException {
        String resolvedIdentifier = resolveGene(geneSymbol);
        if (StringUtils.isEmpty(resolvedIdentifier)) {
            return null;
        }
        String geneId = genes.get(resolvedIdentifier);
        if (geneId == null) {
            Item gene = createItem("Gene");
            gene.setAttribute("primaryIdentifier", resolvedIdentifier);
            //gene.setAttribute("symbol", primaryIdentifier);
            //gene.setReference("organism", getOrganism(TAXON_ID));
            store(gene);
            geneId = gene.getIdentifier();
            genes.put(resolvedIdentifier, geneId);
        }
        return geneId;
    }
    //

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

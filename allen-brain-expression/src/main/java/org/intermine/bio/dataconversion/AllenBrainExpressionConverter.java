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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;


/**
 * 
 * @author Julie Sullivan
 */
public class AllenBrainExpressionConverter extends BioDirectoryConverter
{
    //
    private static final String DATASET_TITLE = "Allen Brain Expression data set";
    private static final String DATA_SOURCE_NAME = "Allen Brain Atlas";
    private static final String TAXON_ID = "9606";
    private String organism;

    private static final String EXPRESSION_FILE = "MicroarrayExpression.csv";
    private static final String ONTOLOGY_FILE = "Ontology.csv";
    private static final String PROBES_FILE = "Probes.csv";
    private static final String SAMPLES_FILE = "SampleAnnot.csv";
    private static final String PACALL_FILE = "PACall.csv";

    private Map<String, Item> genes = new HashMap<String, Item>();
    private Map<String, Item> probes = new HashMap<String, Item>();

    /**
     * Constructor
     * @param writer the ItemWriter used to handle the resultant items
     * @param model the Model
     */
    public AllenBrainExpressionConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
    }

    /**
     * 
     *
     * {@inheritDoc}
     */
    public void process(File dataDir) throws Exception {

        Map<String, File> files = readFilesInDir(dataDir);

        organism = getOrganism(TAXON_ID);

        // don't change order
        // process probes first!
        processProbes(new FileReader(files.get(PROBES_FILE)));
//        processPACall(new FileReader(files.get(PACALL_FILE)));
        processExpression(new FileReader(files.get(EXPRESSION_FILE)));
//        processSamples(new FileReader(files.get(SAMPLES_FILE)));
//        processOntology(new FileReader(files.get(ONTOLOGY_FILE)));
    }

    private void processExpression(Reader reader) throws IOException, ObjectStoreException {
        Iterator<String[]> lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);
        while (lineIter.hasNext()) {
            String[] line = lineIter.next();
            String probeId = line[0];
            Item probe = createItem("Probe");
            probe.setAttribute("probe_id", probeId);
            store(probe);
        }
    }

    // probe_id,probe_name,gene_id,gene_symbol,gene_name,entrez_id,chromosome
    // 1058685,"A_23_P20713",729,"C8G","complement component 8, gamma polypeptide",733,9
    private void processProbes(Reader reader) throws IOException, ObjectStoreException {
        Iterator<String[]> lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);
        // skip header
        lineIter.next();
        while (lineIter.hasNext()) {
            String[] line = lineIter.next();
            String probeId = line[0];
            String name = line[1];
            String entrezId = line[5];

            String geneId = getGene(entrezId);

            Item probe = createItem("Probe");
            probe.setAttribute("probe_id", probeId);
            probe.setAttribute("name", probeId);
            store(probe);
        }
    }

    private String getGene(String entrezId) throws ObjectStoreException {

        String refId = genes.get(entrezGeneNumber);
        if (refId == null) {
            Item gene = createItem("Gene");
            gene.setAttribute("primaryIdentifier", entrezGeneNumber);
            gene.setReference("organism", organism);
            store(gene);
            refId = gene.getIdentifier();
            genes.put(entrezGeneNumber, refId);
        }

        return refId;
    }

    private Map<String, File> readFilesInDir(File dir) {
        Map<String, File> files = new HashMap<String, File>();
        for (File file : dir.listFiles()) {
            files.put(file.getName(), file);
        }
        return files;
    }



}

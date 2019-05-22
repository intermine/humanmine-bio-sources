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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;


/**
 * A data parser to read in Allen Brain Expression data files
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

    private Map<String, String> genes = new HashMap<String, String>();
    private List<String> samples = new LinkedList<String>();
    private Map<String, String> structures = new HashMap<String, String>();
    private Map<String, String> probes = new HashMap<String, String>();
    private Map<String, LinkedList<Item>> probeResults
            = new LinkedHashMap<String, LinkedList<Item>>();

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
        processProbes(new FileReader(files.get(PROBES_FILE)));
        processSamples(new FileReader(files.get(SAMPLES_FILE)));
        processExpression(new FileReader(files.get(EXPRESSION_FILE)));
        processPACall(new FileReader(files.get(PACALL_FILE)));
//        processOntology(new FileReader(files.get(ONTOLOGY_FILE)));
    }

    // 1058685,3.5010774998847847,4.15417262910917
    private void processExpression(Reader reader) throws IOException, ObjectStoreException {
        Iterator<String[]> lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);
        while (lineIter.hasNext()) {
            String[] line = lineIter.next();
            String probe = probes.get(line[0]);
            if (probe == null) {
                throw new RuntimeException("Probe not found" + line[0]);
            }
            LinkedList<Item> currentProbeResults = new LinkedList<Item>();

            // loop through each column
            // samples is in order, so will match with the result
            for (int i = 1; i < samples.size(); i++) {
                String sample = samples.get(i);
                String result = line[i];

                Item probeResult = createItem("ProbeResult");
                probeResult.setAttribute("value", result);
                probeResult.setReference("probe", probe);
                probeResult.setReference("sample", sample);
                currentProbeResults.add(probeResult);
            }
            probeResults.put(probe, currentProbeResults);
        }
    }

    private void processPACall(Reader reader) throws IOException, ObjectStoreException {
        Iterator<String[]> lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);
        while (lineIter.hasNext()) {
            String[] line = lineIter.next();
            String probe = probes.get(line[0]);
            if (probe == null) {
                throw new RuntimeException("Probe not found:" + line[0]);
            }
            List<Item> results = probeResults.get(probe);

            // loop through each column
            // samples is in order, so will match with the result
            for (int i = 1; i < results.size(); i++) {
                Item probeResult = results.get(i);
                String pacall = line[i];
                probeResult.setAttribute("PACall", pacall);
                if ("0".equals(pacall)) {
                    probeResult.removeAttribute("value");
                }
                store(probeResult);
            }
        }
    }


    // structure_id,slab_num,well_id,slab_type,structure_acronym,structure_name,polygon_id,mri_voxel_x,mri_voxel_y,mri_voxel_z,mni_x,mni_y,mni_z
    // 4143,9,11281,CX,"MTG-i","middle temporal gyrus, left, inferior bank of gyrus",1283581,149,106,137,-58.0,-46.0,3.0
    private void processSamples(Reader reader) throws IOException, ObjectStoreException {
        Iterator<String[]> lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);
        // skip header
        lineIter.next();
        while (lineIter.hasNext()) {
            String[] line = lineIter.next();
            String structure = line[0];

            // sample
            String slab_num = line[1];
            String well_id = line[2];
            String slab_type = line[3];

            // structure
            String structure_acronym = line[4];
            String structure_name = line[5];

            // location
            String polygon_id = line[6];
            String mri_voxel_x = line[7];
            String mri_voxel_y = line[8];
            String mri_voxel_z = line[9];
            String mni_x = line[10];
            String mni_y = line[11];
            String mni_z = line[12];

            String structureId = getStructure(structure, structure_acronym, structure_name);

            Item location = createItem("BrainLocation");
            location.setAttribute("polygon_id", polygon_id);
            location.setAttribute("mri_voxel_x", mri_voxel_x);
            location.setAttribute("mri_voxel_y", mri_voxel_y);
            location.setAttribute("mri_voxel_z", mri_voxel_z);
            location.setAttribute("mni_x", mni_x);
            location.setAttribute("mni_y", mni_y);
            location.setAttribute("mni_z", mni_z);
            store(location);

            Item sample = createItem("Sample");
            sample.setAttribute("slab_num", slab_num);
            sample.setAttribute("well_id", well_id);
            sample.setAttribute("slab_type", slab_type);
            sample.setReference("structure", structureId);
            sample.setReference("location", location);
            store(sample);
            samples.add(sample.getIdentifier());
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
            probe.setAttribute("name", name);
            probe.setReference("gene", geneId);
            store(probe);
            probes.put(probeId, probe.getIdentifier());
        }
    }

    /*
        if gene ID is new, create a gene object and store to the database
        if gene is old, return the ID of the stored gene.
     */
    private String getGene(String entrezId) throws ObjectStoreException {
        String refId = genes.get(entrezId);
        if (refId == null) {
            Item gene = createItem("Gene");
            gene.setAttribute("primaryIdentifier", entrezId);
            gene.setReference("organism", organism);
            store(gene);
            refId = gene.getIdentifier();
            genes.put(entrezId, refId);
        }
        return refId;
    }

    private String getStructure(String identifier, String structure_acronym, String structure_name)
        throws ObjectStoreException {
        String refId = structures.get(identifier);
        if (refId == null) {
            Item item = createItem("BrainAtlasStructure");
            item.setAttribute("identifier", identifier);
            item.setAttribute("name", structure_name);
            store(item);
            refId = item.getIdentifier();
            structures.put(identifier, refId);
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

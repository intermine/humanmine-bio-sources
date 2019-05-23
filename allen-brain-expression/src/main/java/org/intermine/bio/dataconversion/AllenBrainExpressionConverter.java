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
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Attribute;
import org.intermine.xml.full.Item;


/**
 * A data parser to read in Allen Brain Expression data files
 *
 * @author Julie Sullivan
 */
public class AllenBrainExpressionConverter extends BioDirectoryConverter
{
    //private static final Logger LOG = Logger.getLogger(AllenBrainExpressionConverter.class);
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
    private Map<String, HashSet<String>> geneToProbe = new HashMap<String, HashSet<String>>();
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
     * {@inheritDoc}
     */
    public void process(File dataDir) throws Exception {
        organism = getOrganism(TAXON_ID);

        // get list of subdirectories
        File[] directories = new File(dataDir.getAbsolutePath()).listFiles(File::isDirectory);

        // process each directory
        for (File f: directories) {

            // get list of files
            Map<String, File> files = readFilesInDir(f);

            // don't change order
            processProbes(new FileReader(files.get(PROBES_FILE)));
            processSamples(new FileReader(files.get(SAMPLES_FILE)));
            processExpression(new FileReader(files.get(EXPRESSION_FILE)),
                    new FileReader(files.get(PACALL_FILE)));
//            processPACall(new FileReader(files.get(PACALL_FILE)));
//        processOntology(new FileReader(files.get(ONTOLOGY_FILE)));
            //createExpressionResults();
            reset();
        }
    }

    private void reset() {
        genes = new HashMap<String, String>();
        samples = new LinkedList<String>();
        structures = new HashMap<String, String>();
        probes = new HashMap<String, String>();
        geneToProbe = new HashMap<String, HashSet<String>>();
        probeResults = new LinkedHashMap<String, LinkedList<Item>>();
    }

    // 1058685,3.5010774998847847,4.15417262910917
    private void processExpression(Reader reader, Reader paReader)
        throws IOException, ObjectStoreException {
        Iterator<String[]> lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);
        Iterator<String[]> palineIter = FormattedTextParser.parseCsvDelimitedReader(paReader);

        while (lineIter.hasNext()) {
            String[] line = lineIter.next();
            String[] paline = palineIter.next();
            String probeIdentifier = line[0];
            String probeRefId = probes.get(probeIdentifier);
            if (probeRefId == null) {
                // probe is not of interest, didn't have an associated gene
                continue;
            }
            LinkedList<Item> currentProbeResults = new LinkedList<Item>();

            // loop through each column
            // samples is in order, so will match with the result
            for (int i = 0; i < samples.size(); i++) {
                String sample = samples.get(i);
                String result = line[i + 1];
                String pacall = paline[i + 1];

                Item probeResult = createItem("ProbeResult");
                if ("1".equals(pacall)) {
                    probeResult.setAttribute("expressionValue", result);
                }
                probeResult.setReference("probe", probeRefId);
                probeResult.setReference("sample", sample);
                probeResult.setAttribute("PACall", pacall);
                store(probeResult);
                //currentProbeResults.add(probeResult);
            }
            //probeResults.put(probeIdentifier, currentProbeResults);
        }

    }

    private void createExpressionResults() throws ObjectStoreException {
        // samples to the list of probeResults
        Map<String, List<Item>> sampleResults = new HashMap<String, List<Item>>();
        // we want to do calcs for all results for each gene
        for (Map.Entry<String, HashSet<String>> entry : geneToProbe.entrySet()) {
            String geneRefId = entry.getKey();
            HashSet<String> geneProbes = entry.getValue();

            // each gene will have several probes
            for (String probeIdentifier : geneProbes) {
                List<Item> resultsForThisProbe = probeResults.get(probeIdentifier);
                for (int i = 0; i < resultsForThisProbe.size(); i++) {
                    Item probeResult = resultsForThisProbe.get(i);
                    List<Item> sampleResultsForProbe = sampleResults.get("Sample"
                            + String.valueOf(i));
                    if (sampleResultsForProbe == null) {
                        sampleResultsForProbe = new ArrayList<Item>();
                    }
                    sampleResultsForProbe.add(probeResult);
                    sampleResults.put("Sample" + String.valueOf(i), sampleResultsForProbe);
                }
            }

            // we've gathered up all of the information. do the math now!
            for (int i = 0; i < samples.size(); i++) {
                Item expressionResult = createItem("ExpressionResult");
                expressionResult.setReference("gene", geneRefId);
                expressionResult.setReference("sample", samples.get(i));
                List<Item> sampleProbeResults = sampleResults.get("Sample" + String.valueOf(i));
                Set<String> expressionValues = new HashSet<String>();
                for (Item item : sampleProbeResults) {
                    if (item.hasAttribute("expressionValue")) {
                        Attribute attr = item.getAttribute("expressionValue");
                        String expressionValue = attr.getValue();
                        expressionValues.add(expressionValue);
                    }
                    expressionResult.addToCollection("probeResults", item);
                }
                BigDecimal averagedExpression = getAveragedExpression(expressionValues);
                if (!averagedExpression.equals(BigDecimal.ZERO)) {
                    expressionResult.setAttribute("averagedExpression",
                        averagedExpression.toString());
                }
                store(expressionResult);
            }
        }
    }

    private BigDecimal getAveragedExpression(Set<String> expressionValues) {
        int count = expressionValues.size();
        if (count == 0) {
            return BigDecimal.ZERO;
        }
        Set<BigDecimal> decimalValues = new HashSet<BigDecimal>();
        for (String expressionValue : expressionValues) {
            BigDecimal decimalValue = new BigDecimal(expressionValue);
            decimalValues.add(decimalValue);
        }
        BigDecimal sum = decimalValues.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal averagedExpression = sum.divide(new BigDecimal(count), 2);
        return averagedExpression;
    }

    private void processSamples(Reader reader) throws IOException, ObjectStoreException {
        Iterator<String[]> lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);
        // skip header
        lineIter.next();
        while (lineIter.hasNext()) {
            String[] line = lineIter.next();
            String structure = line[0];

            // sample
            String slabnum = line[1];
            String wellid = line[2];
            String slabtype = line[3];

            // structure
//            String structure_acronym = line[4];
            String structurename = line[5];

            // location
            String polygonId = line[6];
            String mrivoxelx = line[7];
            String mrivoxely = line[8];
            String mrivoxelz = line[9];
            String mnix = line[10];
            String mniy = line[11];
            String mniz = line[12];

            String structureId = getStructure(structure, structurename);

            Item location = createItem("BrainLocation");
            location.setAttribute("polygon_id", polygonId);
            location.setAttribute("mri_voxel_x", mrivoxelx);
            location.setAttribute("mri_voxel_y", mrivoxely);
            location.setAttribute("mri_voxel_z", mrivoxelz);
            location.setAttribute("mni_x", mnix);
            location.setAttribute("mni_y", mniy);
            location.setAttribute("mni_z", mniz);
            store(location);

            Item sample = createItem("Sample");
            sample.setAttribute("slab_num", slabnum);
            sample.setAttribute("well_id", wellid);
            sample.setAttribute("slab_type", slabtype);
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
            String probeIdentifier = line[0];
            String name = line[1];
            String symbol = line[3];
            String entrezId = line[5];

            // Miguel says to ignore (for now) probes that don't have an associated gene
            if (StringUtils.isEmpty(symbol)) {
                continue;
            }

            String geneRefId = getGene(entrezId, symbol);

            Item probe = createItem("Probe");
            probe.setAttribute("probe_id", probeIdentifier);
            probe.setAttribute("name", name);
            probe.setReference("gene", geneRefId);
            store(probe);
            probes.put(probeIdentifier, probe.getIdentifier());

            HashSet<String> probeSets = geneToProbe.get(geneRefId);
            if (probeSets == null) {
                probeSets = new HashSet<String>();
            }
            probeSets.add(probeIdentifier);
            geneToProbe.put(geneRefId, probeSets);
        }
    }

    /*
        if gene ID is new, create a gene object and store to the database
        if gene is old, return the ID of the stored gene.
     */
    private String getGene(String entrezId, String symbol) throws ObjectStoreException {
        String refId = genes.get(entrezId);
        if (refId == null) {
            Item gene = createItem("Gene");
            if (StringUtils.isNotEmpty(entrezId)) {
                gene.setAttribute("primaryIdentifier", entrezId);
            }
            gene.setAttribute("symbol", symbol);
            gene.setReference("organism", organism);
            store(gene);
            refId = gene.getIdentifier();
            genes.put(entrezId, refId);
        }
        return refId;
    }

    private String getStructure(String identifier, String name)
        throws ObjectStoreException {
        String refId = structures.get(identifier);
        if (refId == null) {
            Item item = createItem("BrainAtlasStructure");
            item.setAttribute("identifier", identifier);
            item.setAttribute("name", name);
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

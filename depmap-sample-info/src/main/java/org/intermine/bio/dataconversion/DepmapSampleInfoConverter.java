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
import java.util.Map;

import org.apache.log4j.Logger;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;


/**
 * 
 * @author
 */
public class DepmapSampleInfoConverter extends BioDirectoryConverter
{
    protected static final Logger LOG = Logger.getLogger(DepmapSampleInfoConverter.class);

    private static final String DATASET_TITLE = "depmap-sample-info";
    private static final String DATA_SOURCE_NAME = "depmap";

    private static final String TAXON_ID = "9606"; // Human Taxon ID

    private static final String SAMPLE_INFO_CSV_FILE = "sample_info.csv";

    private Map<String, String> cellLines = new HashMap<String, String>();

    private String organismIdentifier; // Not the taxon ID. It references the object that is created into the database.

    /**
     * Constructor
     * @param writer the ItemWriter used to handle the resultant items
     * @param model the Model
     */
    public DepmapSampleInfoConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
    }

    /**
     * 
     *
     * {@inheritDoc}
     */
    public void process(File dataDir) throws Exception {

        Map<String, File> files = readFilesInDir(dataDir);

        organismIdentifier = getOrganism(TAXON_ID);

        processCellLines(new FileReader(files.get(SAMPLE_INFO_CSV_FILE)));

    }

    private Map<String, File> readFilesInDir(File dir) {
        Map<String, File> files = new HashMap<String, File>();
        for (File file : dir.listFiles()) {
            files.put(file.getName(), file);
        }
        return files;
    }

    private void processCellLines(Reader reader) throws ObjectStoreException, IOException {
        Iterator<?> lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);
        // Skip header
        lineIter.next();
        int counter = 0;
        while (lineIter.hasNext()) {
            counter++;
            LOG.info("AAA " + counter);
            String[] line = (String[]) lineIter.next();

            String DepMapID = line[0];

            if(cellLines.containsKey(DepMapID)) {
                continue;
            }

            // it seems there are mis-alignments
            String ShortName = line[1];
            String CCLEname = line[3]; //2
            String Lineage = line[22]; //21
            String LineageSubtype = line[23];
            String LineageSubsubtype = line[24];
            String Sex = line[6]; //5
            String PrimaryOrMetastasis = line[16]; //14
            String Disease = line[17]; //15
            String DiseaseSubtype = line[18]; //17
            String Age = "Not specified";
            if(!line[19].isEmpty() && line[19].length() < 4) { // to avoid strings; e.g. "Unknown"
                Age = Double.toString(Math.floor(Double.valueOf(line[19])));
            }

            if(Lineage.isEmpty()) {
                continue;
            }

            /*if(!line[20].isEmpty()) {
                Age = Double.parseDouble(line[20]);
            }*/

            Item cellLineItem;

            cellLineItem = createItem("CellLine");

            cellLineItem.setAttribute("primaryIdentifier", DepMapID);

            if(!DepMapID.isEmpty()) {
                cellLineItem.setAttribute("DepMapID", DepMapID);
            } else {
                continue;
            }

            if(!CCLEname.isEmpty()) {
                cellLineItem.setAttribute("CCLEname", CCLEname);
            } else {
                cellLineItem.setAttribute("CCLEname", "Not specified");
            }

            if(!ShortName.isEmpty()) {
                cellLineItem.setAttribute("ShortName", ShortName);
            } else {
                cellLineItem.setAttribute("ShortName", "Not specified");
            }

            if(!Lineage.isEmpty()) {
                cellLineItem.setAttribute("Lineage", Lineage);
            } else {
                continue;
            }

            if(!LineageSubtype.isEmpty()) {
                cellLineItem.setAttribute("LineageSubtype", LineageSubtype);
            } else {
                cellLineItem.setAttribute("LineageSubtype", "Not specified");
            }

            if(!LineageSubsubtype.isEmpty()) {
                cellLineItem.setAttribute("LineageSubsubtype", LineageSubsubtype);
            } else {
                cellLineItem.setAttribute("LineageSubsubtype", "Not specified");
            }

            if(!Sex.isEmpty()) {
                cellLineItem.setAttribute("Sex", Sex);
            } else {
                cellLineItem.setAttribute("Sex", "Not specified");
            }

            if(!PrimaryOrMetastasis.isEmpty()) {
                cellLineItem.setAttribute("PrimaryOrMetastasis", PrimaryOrMetastasis);
            } else {
                cellLineItem.setAttribute("PrimaryOrMetastasis", "Not specified");
            }

            if(!Disease.isEmpty()) {
                cellLineItem.setAttribute("Disease", Disease);
            } else {
                continue;
            }

            if(!DiseaseSubtype.isEmpty()) {
                cellLineItem.setAttribute("DiseaseSubtype", DiseaseSubtype);
            } else {
                cellLineItem.setAttribute("DiseaseSubtype", "Not specified");
            }
            //cellLineItem.setAttribute("Age", Double.toString(Age));
            if(!Age.isEmpty()) {
                cellLineItem.setAttribute("Age", Age);
            } else {
                cellLineItem.setAttribute("Age", "Not specified");
            }

            cellLineItem.setReference("organism", getOrganism(TAXON_ID));

            store(cellLineItem);
            cellLines.put(DepMapID, cellLineItem.getIdentifier());
        }
    }
}

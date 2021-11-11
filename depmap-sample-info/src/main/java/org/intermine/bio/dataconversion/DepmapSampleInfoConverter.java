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
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;

/**
 * @author abzg
 */
public class DepmapSampleInfoConverter extends BioDirectoryConverter
{
    protected static final Logger LOG = Logger.getLogger(DepmapSampleInfoConverter.class);

    private static final String DATASET_TITLE = "depmap-sample-info";
    private static final String DATA_SOURCE_NAME = "depmap";

    private static final String TAXON_ID = "9606"; // Human Taxon ID
    private static final String SAMPLE_INFO_CSV_FILE = "sample_info.csv";

    private Map<String, String> cellLines = new HashMap<String, String>();
    private String organismIdentifier; // references the object in the database.

    /**
     * Constructor
     *
     * @param writer the ItemWriter used to handle the resultant items
     * @param model  the Model
     */
    public DepmapSampleInfoConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
    }

    /**
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
            String[] line = (String[]) lineIter.next();
            String depMapID = line[0];
            if (cellLines.containsKey(depMapID)) {
                continue;
            }
            // it seems there are mis-alignments
            String shortName = line[1];
            String ccleName = line[3]; //2
            String lineage = line[22]; //21
            String lineageSubtype = line[23];
            String lineageSubsubtype = line[24];
            String sex = line[6]; //5
            String primaryOrMetastasis = line[16]; //14
            String disease = line[17]; //15
            String diseaseSubtype = line[18]; //17
            String age = "Not specified";
            if (!line[19].isEmpty() && line[19].length() < 4) { // to avoid strings; e.g. "Unknown"
                age = Double.toString(Math.floor(Double.valueOf(line[19])));
            }
            if (lineage.isEmpty()) {
                continue;
            }

            /*if(!line[20].isEmpty()) {
                Age = Double.parseDouble(line[20]);
            }*/

            Item cellLineItem;
            cellLineItem = createItem("CellLine");
            cellLineItem.setAttribute("primaryIdentifier", depMapID);
            if (!depMapID.isEmpty()) {
                cellLineItem.setAttribute("DepMapID", depMapID);
            } else {
                continue;
            }
            if (!ccleName.isEmpty()) {
                cellLineItem.setAttribute("CCLEname", ccleName);
            } else {
                cellLineItem.setAttribute("CCLEname", "Not specified");
            }
            if (!shortName.isEmpty()) {
                cellLineItem.setAttribute("ShortName", shortName);
            } else {
                cellLineItem.setAttribute("ShortName", "Not specified");
            }
            if (!lineage.isEmpty()) {
                cellLineItem.setAttribute("Lineage", lineage);
            } else {
                continue;
            }
            if (!lineageSubtype.isEmpty()) {
                cellLineItem.setAttribute("LineageSubtype", lineageSubtype);
            } else {
                cellLineItem.setAttribute("LineageSubtype", "Not specified");
            }
            if (!lineageSubsubtype.isEmpty()) {
                cellLineItem.setAttribute("LineageSubsubtype", lineageSubsubtype);
            } else {
                cellLineItem.setAttribute("LineageSubsubtype", "Not specified");
            }
            if (!sex.isEmpty()) {
                cellLineItem.setAttribute("Sex", sex);
            } else {
                cellLineItem.setAttribute("Sex", "Not specified");
            }
            if (!primaryOrMetastasis.isEmpty()) {
                cellLineItem.setAttribute("PrimaryOrMetastasis", primaryOrMetastasis);
            } else {
                cellLineItem.setAttribute("PrimaryOrMetastasis", "Not specified");
            }
            if (!disease.isEmpty()) {
                cellLineItem.setAttribute("Disease", disease);
            } else {
                continue;
            }
            if (!diseaseSubtype.isEmpty()) {
                cellLineItem.setAttribute("DiseaseSubtype", diseaseSubtype);
            } else {
                cellLineItem.setAttribute("DiseaseSubtype", "Not specified");
            }
            //cellLineItem.setAttribute("Age", Double.toString(Age));
            if (!age.isEmpty()) {
                cellLineItem.setAttribute("Age", age);
            } else {
                cellLineItem.setAttribute("Age", "Not specified");
            }

            cellLineItem.setReference("organism", getOrganism(TAXON_ID));
            store(cellLineItem);
            cellLines.put(depMapID, cellLineItem.getIdentifier());
        }
    }
}

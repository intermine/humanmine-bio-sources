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

import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;

/**
 * Read the HuGE GWAS flat file and create GWAS items and GWASResults.
 * @author Richard Smith
 */
public class HugeGwasConverter extends BioFileConverter
{
    //
    private static final String DATASET_TITLE = "huge-gwas";
    private static final String DATA_SOURCE_NAME = "GWAS catalog";

    private Map<String, String> genes = new HashMap<String, String>();
    private Map<String, String> pubs = new HashMap<String, String>();
    private Map<String, String> snps = new HashMap<String, String>();
    private Map<String, String> studies = new HashMap<String, String>();

    private static final String HUMAN_TAXON = "9606";

    // approximately the minimum permitted double value in postgres
    private static final double MIN_POSTGRES_DOUBLE = 1.0E-307;

    private static final Logger LOG = Logger.getLogger(HugeGwasConverter.class);

    protected IdResolver rslv;

    /**
     * Constructor
     * @param writer the ItemWriter used to handle the resultant items
     * @param model the Model
     */
    public HugeGwasConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
    }

    /**
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {

        if (rslv == null) {
            rslv = IdResolverService.getIdResolverByTaxonId(HUMAN_TAXON, false);
        }

        Iterator<?> lineIter = FormattedTextParser.parseTabDelimitedReader(reader);

        // skip header
        lineIter.next();

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();
            if (line.length <= 1) {
                continue;
            }

            String pubIdentifier = getPub(line[1]);
            String geneIdentifier = getGene(line[17]);
            String snp = line[21];

            // gwas
            String firstAuthor = line[2];
            String initialSample = line[8];
            String name = line[6];
            String replicateSample = line[9];
            String expDate = line[3];
            String year = null;
            if (expDate != null) {
                year = expDate.substring(0, 4);
            }

            // result
            String associatedVariantRiskAllele = line[20];
            String phenotype = line[7];
            Double pValue = parsePValue(line[27]);
            String riskAlleleFreqInControls = line[26];

            Item result = createItem("GWASResult");
            result.setReference("SNP", getSnpIdentifier(snp));
            if (geneIdentifier != null) {
                result.addToCollection("associatedGenes", geneIdentifier);
            }
            result.setAttribute("phenotype", phenotype);
            if (pValue != null) {
                result.setAttribute("pValue", pValue.toString());
            }
            String studyIdentifier = getStudy(pubIdentifier, firstAuthor, year, name,
                    initialSample, replicateSample);
            result.setReference("study", studyIdentifier);

            String allele = parseAllele(associatedVariantRiskAllele, snp);
            if (StringUtils.isNotEmpty(allele)) {
                result.setAttribute("associatedVariantRiskAllele", allele);
            }
            if (StringUtils.isNotEmpty(riskAlleleFreqInControls)) {
                try {
                    Float.parseFloat(riskAlleleFreqInControls);
                    result.setAttribute("riskAlleleFreqInControls",
                            riskAlleleFreqInControls);
                } catch (NumberFormatException e) {
                    // wasn't a valid float, probably "NR"
                }
            }
            store(result);
        }
    }

    /**
     * Read a p-value from a String of the format 5x10-6.
     * @param s the input string, e.g. 5x10-6
     * @return the extracted double or null if failed to parse
     */
    protected Double parsePValue(String s) {
        s = s.replace("x10", "E");
        try {
            double pValue = Double.parseDouble(s);

            // Postgres JDBC driver is allowing double values outside the permitted range to be
            // stored which are then unusable.  This a hack to prevent it.
            if (pValue < MIN_POSTGRES_DOUBLE) {
                pValue = 0.0;
            }

            return pValue;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String getStudy(String pubIdentifier, String firstAuthor, String year, String name,
            String initialSample, String replicateSample)
        throws ObjectStoreException {
        String studyIdentifier = studies.get(pubIdentifier);
        if (studyIdentifier == null) {
            Item gwas = createItem("GWAS");
            gwas.setAttribute("firstAuthor", firstAuthor);
            gwas.setAttribute("year", year);
            gwas.setAttribute("name", name);
            gwas.setAttribute("initialSample", initialSample);
            if (StringUtils.isNotEmpty(replicateSample)) {
                gwas.setAttribute("replicateSample", replicateSample);
            }
            gwas.setReference("publication", pubIdentifier);
            store(gwas);

            studyIdentifier = gwas.getIdentifier();
            studies.put(pubIdentifier, studyIdentifier);
        }
        return studyIdentifier;
    }

    private String getPub(String pubMedId) throws ObjectStoreException {
        String pubIdentifier = pubs.get(pubMedId);
        if (pubIdentifier == null) {
            Item pub = createItem("Publication");
            pub.setAttribute("pubMedId", pubMedId);
            store(pub);

            pubIdentifier = pub.getIdentifier();
            pubs.put(pubMedId, pubIdentifier);
        }
        return pubIdentifier;
    }

    private String parseAllele(String snpIdentifier, String rsNumber) {
        if (snpIdentifier.startsWith("rs")) {
            if (snpIdentifier.indexOf('-') >= 0) {
                String riskSnp = snpIdentifier.substring(0, snpIdentifier.indexOf('-'));
                if (riskSnp.equals(rsNumber)) {
                    String allele = snpIdentifier.substring(snpIdentifier.indexOf('-') + 1);
                    return allele;
                }
            }
        }
        return null;
    }

    private String getSnpIdentifier(String rsNumber) throws ObjectStoreException {
        if (!snps.containsKey(rsNumber)) {
            Item snp = createItem("SNP");
            snp.setAttribute("primaryIdentifier", rsNumber);
            snp.setReference("organism", getOrganism(HUMAN_TAXON));
            store(snp);
            snps.put(rsNumber, snp.getIdentifier());
        }
        return snps.get(rsNumber);
    }

    private String getGene(String ensemblIdentifier) throws ObjectStoreException {
        String identifier = resolveGene(ensemblIdentifier);
        if (identifier == null) {
            return null;
        }

        String geneIdentifier = genes.get(identifier);
        if (geneIdentifier == null) {
            Item gene = createItem("Gene");
            gene.setAttribute("primaryIdentifier", identifier);
            gene.setReference("organism", getOrganism(HUMAN_TAXON));
            geneIdentifier = gene.getIdentifier();
            store(gene);
            genes.put(identifier, geneIdentifier);
        }
        return geneIdentifier;
    }

    private String resolveGene(String identifier) {
        String id = identifier;

        if (rslv != null && rslv.hasTaxon(HUMAN_TAXON)) {
            int resCount = rslv.countResolutions(HUMAN_TAXON, identifier);
            if (resCount != 1) {
                LOG.info("RESOLVER: failed to resolve gene to one identifier, ignoring gene: "
                         + identifier + " count: " + resCount + " Human identifier: "
                         + rslv.resolveId(HUMAN_TAXON, identifier));
                return null;
            }
            id = rslv.resolveId(HUMAN_TAXON, identifier).iterator().next();
        }
        return id;
    }
}

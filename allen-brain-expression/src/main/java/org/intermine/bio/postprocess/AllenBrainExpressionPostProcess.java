package org.intermine.bio.postprocess;

/*
 * Copyright (C) 2002-2019 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import org.apache.log4j.Logger;
import org.intermine.bio.util.Constants;
import org.intermine.metadata.ConstraintOp;
import org.intermine.model.bio.ExpressionResult;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.ProbeResult;
import org.intermine.model.bio.Probe;
import org.intermine.model.bio.Sample;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.postprocess.PostProcessor;
import org.intermine.util.DynamicUtil;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * For all genes + probes + samples, take the average expression score and save as a new object.
 * @author julie sullivan
 */
public class AllenBrainExpressionPostProcess extends PostProcessor
{
    private static final Logger LOG = Logger.getLogger(AllenBrainExpressionPostProcess.class);
    protected ObjectStore os;

    /**
     * @param osw writer on genomic ObjectStore
     */
    public AllenBrainExpressionPostProcess(ObjectStoreWriter osw) {
        super(osw);
        this.os = osw.getObjectStore();
    }

    /**
     * Get all probe results (per gene + sample/probe combo), average score and save a new object.
     *
     * @throws ObjectStoreException if anything goes wrong
     */
    @Override
    public void postProcess() throws ObjectStoreException {

        long startTime = System.currentTimeMillis();

        osw.beginTransaction();

        Iterator<?> resIter = runProbeQuery();

        int count = 0;
        Gene lastGene = null;
        Sample lastSample = null;
        Set<ProbeResult> probeResults = new HashSet<ProbeResult>();

        while (resIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) resIter.next();
            Gene thisGene = (Gene) rr.get(0);
            ProbeResult thisProbeResult = (ProbeResult) rr.get(1);
            Sample thisSample = (Sample) rr.get(2);

            // save when the SAMPLE changes. The query is ordered by GENE then SAMPLE
            // so when sample changes, will be on the same gene
            if (lastSample != null && !(lastSample.equals(thisSample))) {

                ExpressionResult expressionResult = (ExpressionResult) DynamicUtil
                        .createObject(Collections.singleton(ExpressionResult.class));
                expressionResult.setGene(lastGene);
                expressionResult.setSample(lastSample);
                expressionResult.setProbeResults(probeResults);

                Set<Double> expressionValues = new HashSet<Double>();
                for (ProbeResult probeResult : probeResults) {
                    if (probeResult.getExpressionValue() != null) {
                        expressionValues.add(probeResult.getExpressionValue());
                    }
                }
                BigDecimal averagedExpression = getAveragedExpression(expressionValues);
                if (!averagedExpression.equals(BigDecimal.ZERO)) {
                    expressionResult.setAveragedExpression(averagedExpression.doubleValue());
                }
                osw.store(expressionResult);
            }

            probeResults.add(thisProbeResult);

            lastGene = thisGene;
            lastSample = thisSample;
            count++;
        }

        if (lastSample != null) {
            ExpressionResult expressionResult = (ExpressionResult) DynamicUtil
                    .createObject(Collections.singleton(ExpressionResult.class));
            expressionResult.setGene(lastGene);
            expressionResult.setSample(lastSample);
            expressionResult.setProbeResults(probeResults);

            Set<Double> expressionValues = new HashSet<Double>();
            for (ProbeResult probeResult : probeResults) {
                if (probeResult.getExpressionValue() != null) {
                    expressionValues.add(probeResult.getExpressionValue());
                }
            }
            BigDecimal averagedExpression = getAveragedExpression(expressionValues);
            if (!averagedExpression.equals(BigDecimal.ZERO)) {
                expressionResult.setAveragedExpression(averagedExpression.doubleValue());
            }
            osw.store(expressionResult);
        }

        LOG.info("Created " + count + " new expression results "
                + " - took " + (System.currentTimeMillis() - startTime) + " ms.");
        osw.commitTransaction();
    }

    private BigDecimal getAveragedExpression(Set<Double> expressionValues) {
        int count = expressionValues.size();
        if (count == 0) {
            return BigDecimal.ZERO;
        }
        Set<BigDecimal> decimalValues = new HashSet<BigDecimal>();
        for (Double expressionValue : expressionValues) {
            BigDecimal decimalValue = new BigDecimal(expressionValue);
            decimalValues.add(decimalValue);
        }
        BigDecimal sum = decimalValues.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal averagedExpression = sum.divide(new BigDecimal(count), 2);
        return averagedExpression;
    }

    /**
     *  Gene --> Probes --> ProbeResult --> Sample
     */
    private Iterator<?> runProbeQuery() throws ObjectStoreException {
        Query q = new Query();

        q.setDistinct(false);

        QueryClass qcGene = new QueryClass(Gene.class);
        q.addFrom(qcGene);
        q.addToSelect(qcGene);
        q.addToOrderBy(qcGene);

        QueryClass qcProbe = new QueryClass(Probe.class);
        q.addFrom(qcProbe);

        QueryClass qcProbeResult = new QueryClass(ProbeResult.class);
        q.addFrom(qcProbeResult);
        q.addToSelect(qcProbeResult);

        QueryClass qcSample = new QueryClass(Sample.class);
        q.addFrom(qcSample);
        q.addToSelect(qcSample);
        q.addToOrderBy(qcSample);

        ConstraintSet cs = new ConstraintSet(ConstraintOp.AND);

        // Probe.gene
        QueryObjectReference geneProbeRef = new QueryObjectReference(qcProbe, "gene");
        cs.addConstraint(new ContainsConstraint(geneProbeRef, ConstraintOp.CONTAINS, qcGene));

        // Probe.probeResults
        QueryObjectReference probeResultsToProbeRef =
                new QueryObjectReference(qcProbeResult, "probe");
        cs.addConstraint(new ContainsConstraint(probeResultsToProbeRef, ConstraintOp.CONTAINS,
                qcProbe));

        // Probe.probeResults.sample
        QueryObjectReference sampleRef =
                new QueryObjectReference(qcProbeResult, "sample");
        cs.addConstraint(new ContainsConstraint(sampleRef, ConstraintOp.CONTAINS, qcSample));

        q.setConstraint(cs);

        ((ObjectStoreInterMineImpl) os).precompute(q, Constants.PRECOMPUTE_CATEGORY);
        Results res = os.execute(q, 5000, true, true, true);
        if (res.isEmpty()) {
            throw new RuntimeException("error bad query " + q.toString());
        }
        return res.iterator();
    }

}

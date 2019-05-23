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

import org.custommonkey.xmlunit.XMLTestCase;
import org.intermine.model.InterMineObject;
import org.intermine.model.bio.ExpressionResult;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.ProbeResult;
import org.intermine.model.bio.Probe;
import org.intermine.model.bio.Sample;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.ObjectStoreWriterFactory;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.SingletonResults;
import org.intermine.util.DynamicUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Tests for the GoPostprocess class.
 */
public class AllenBrainExpressionPostprocessTest extends XMLTestCase {

    private ObjectStoreWriter osw;

    public void setUp() throws Exception {
        super.setUp();
        osw = ObjectStoreWriterFactory.getObjectStoreWriter("osw.bio-test");
        osw.getObjectStore().flushObjectById();

    }

    public void tearDown() throws Exception {
        deleteAlltheThings();
        osw.close();
    }

    public void deleteAlltheThings() throws ObjectStoreException {
        Query q = new Query();
        QueryClass qc = new QueryClass(InterMineObject.class);
        q.addFrom(qc);
        q.addToSelect(qc);
        ObjectStore os = osw.getObjectStore();
        SingletonResults res = os.executeSingleton(q);
        Iterator resIter = res.iterator();
        osw.beginTransaction();
        while (resIter.hasNext()) {
            InterMineObject o = (InterMineObject) resIter.next();
            osw.delete(o);
        }
        osw.commitTransaction();
    }

    public void testPostProcess() throws Exception {
        deleteAlltheThings();
        setUpData();
        AllenBrainExpressionPostProcess gp = new AllenBrainExpressionPostProcess(osw);
        gp.postProcess();

        Set<InterMineObject> results = getFromDb(ExpressionResult.class);

        assertEquals(1, results.size());

        for (InterMineObject o : results) {
            ExpressionResult r = (ExpressionResult) o;
            assertEquals(r.getAveragedExpression(), 10.0);
        }
    }

    // Store 2 genes with a protein, each protein has a GO term
    private void setUpData() throws Exception {
        Gene gene1 = (Gene) DynamicUtil.createObject(Collections.singleton(Gene.class));
        gene1.setName("GENE 1");
        Gene gene2 = (Gene) DynamicUtil.createObject(Collections.singleton(Gene.class));
        gene2.setName("GENE 2");
        Probe probe1 = (Probe) DynamicUtil.createObject(Collections.singleton(Probe.class));
        probe1.setGene(gene1);
        Probe probe2 = (Probe) DynamicUtil.createObject(Collections.singleton(Probe.class));
        probe2.setGene(gene2);

        ProbeResult probeResult1 = (ProbeResult) DynamicUtil.createObject(Collections.singleton(ProbeResult.class));
        probeResult1.setExpressionValue(new Double(10));
        probeResult1.setProbe(probe1);
        ProbeResult probeResult2 = (ProbeResult) DynamicUtil.createObject(Collections.singleton(ProbeResult.class));
        probeResult2.setExpressionValue(new Double(20));
        probeResult1.setProbe(probe2);

        Sample sample1 = (Sample) DynamicUtil.createObject(Collections.singleton(Sample.class));
        probeResult1.setSample(sample1);
        Sample sample2 = (Sample) DynamicUtil.createObject(Collections.singleton(Sample.class));
        probeResult2.setSample(sample2);


        List toStore = new ArrayList(Arrays.asList(new Object[] {gene1, gene2, probe1, probe2, probeResult1, probeResult2, sample1, sample2}));

        osw.beginTransaction();
        Iterator i = toStore.iterator();
        while (i.hasNext()) {
            osw.store((InterMineObject) i.next());
        }
        osw.commitTransaction();
    }


    private Set<InterMineObject> getFromDb(Class relClass) throws Exception {
        Query q = new Query();
        QueryClass qc = new QueryClass(relClass);
        q.addToSelect(qc);
        q.addFrom(qc);
        SingletonResults res = osw.getObjectStore().executeSingleton(q);
        Set<InterMineObject> results = new HashSet<InterMineObject>();
        Iterator resIter = res.iterator();
        while(resIter.hasNext()) {
            results.add((InterMineObject) resIter.next());
        }
        ObjectStore os = osw.getObjectStore();
        os.flushObjectById();
        return results;
    }
}

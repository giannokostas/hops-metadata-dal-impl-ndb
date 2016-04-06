package io.hops.metadata.ndb.dalimpl.yarn;

import io.hops.StorageConnector;
import io.hops.metadata.ndb.NdbStorageFactory;
import io.hops.metadata.yarn.dal.YarnApplicationResourcesDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.YarnApplicationResources;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestApplicationResources {

    NdbStorageFactory storageFactory = new NdbStorageFactory();
    StorageConnector connector = storageFactory.getConnector();
    final List<YarnApplicationResources> conts =
            new ArrayList<YarnApplicationResources>();

    @Before
    public void setup() throws IOException{
        storageFactory.setConfiguration(getMetadataClusterConfiguration());
        RequestHandler.setStorageConnector(connector);
        LightWeightRequestHandler setRMDTMasterKeyHandler
                = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.formatStorage();

                return null;
            }
        };
        setRMDTMasterKeyHandler.handle();
    }

    @Test
    public void testApplicationResource() throws IOException{
        final YarnApplicationResources yarn1 =
                new YarnApplicationResources(125, "something1", 512, 1);
        final YarnApplicationResources yarn2 =
                new YarnApplicationResources(163, "something2", 1024, 2);
        final YarnApplicationResources yarn3 =
                new YarnApplicationResources(174, "something3", 2048, 3);

        conts.add(yarn1);
        conts.add(yarn2);
        conts.add(yarn3);

        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                YarnApplicationResourcesDataAccess yarnDataAccess = (YarnApplicationResourcesDataAccess)
                        storageFactory.getDataAccess(YarnApplicationResourcesDataAccess.class);

                yarnDataAccess.add(yarn1);
                yarnDataAccess.add(yarn2);
                yarnDataAccess.add(yarn3);

                connector.commit();

                return null;
            }
        };

        populate.handle();

        LightWeightRequestHandler queryCont = new QueryAppResources(YARNOperationType.TEST);

        Map<Integer, YarnApplicationResources> queryResult =
                (Map<Integer, YarnApplicationResources>) queryCont.handle();

        Assert.assertEquals("should be 3", 3, queryResult.size());

        YarnApplicationResourcesDataAccess pr = (YarnApplicationResourcesDataAccess)
                storageFactory.getDataAccess(YarnApplicationResourcesDataAccess.class);

        YarnApplicationResources theApp = pr.findById(125);
        Assert.assertEquals("Name not equal", "something1" ,theApp.getName());
        Assert.assertEquals("Memory not equal", 512 ,theApp.getAllocated_mb());
    }


    @Test
    public void TestRemoveRecords() throws IOException{
        YarnApplicationResourcesDataAccess prContDAO = (YarnApplicationResourcesDataAccess)
                storageFactory.getDataAccess(YarnApplicationResourcesDataAccess.class);

        //TODO: Remove doesn't work
        prContDAO.removeAll(conts);
    }

    private Properties getMetadataClusterConfiguration()
            throws IOException {
        String configFile = "ndb-config.properties";
        Properties clusterConf = new Properties();
        InputStream inStream = StorageConnector.class.getClassLoader().
                getResourceAsStream(configFile);
        clusterConf.load(inStream);
        return clusterConf;
    }

    private class QueryAppResources extends LightWeightRequestHandler {

        public QueryAppResources(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            YarnApplicationResourcesDataAccess prContDAO = (YarnApplicationResourcesDataAccess)
            storageFactory.getDataAccess(YarnApplicationResourcesDataAccess.class);

            Map<String, YarnApplicationResources> result = prContDAO.getAll();

            connector.commit();

            return result;
            }
    }

}

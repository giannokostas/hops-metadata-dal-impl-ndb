package io.hops.metadata.ndb.dalimpl.yarn;

import io.hops.StorageConnector;
import io.hops.metadata.ndb.NdbStorageFactory;
import io.hops.metadata.yarn.dal.YarnApplicationResourcesDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.YarnApplicationResources;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestApplicationResources {

    NdbStorageFactory storageFactory = new NdbStorageFactory();
    StorageConnector connector = storageFactory.getConnector();
    final Map<String, YarnApplicationResources> appResources =
            new HashMap<String, YarnApplicationResources>();
    final Map<String, YarnApplicationResources> appToBeRemoved =
            new HashMap<String, YarnApplicationResources>();

    @Before
    public void setUp() throws IOException{

        final YarnApplicationResources yarnResource_1 =
                new YarnApplicationResources("app_id_1", 1024, 1);
        final YarnApplicationResources yarnResource_2 =
                new YarnApplicationResources("app_id_2", 1024, 1);
        final YarnApplicationResources yarnResource_3 =
                new YarnApplicationResources("app_id_3", 2048, 2);

        appResources.put("app_id_1", yarnResource_1);
        appResources.put("app_id_2", yarnResource_2);
        appResources.put("app_id_3", yarnResource_3);

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

    @After
    public void tearDown() throws IOException {
        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                YarnApplicationResourcesDataAccess pr = (YarnApplicationResourcesDataAccess)
                        storageFactory.getDataAccess(YarnApplicationResourcesDataAccess.class);

                Map<String, YarnApplicationResources> allRecords = pr.getAll();

                pr.removeAll(allRecords);
                connector.commit();

                return null;
            }
        };

        populate.handle();

        LightWeightRequestHandler queryCont = new QueryAppResources(YARNOperationType.TEST);

        Map<Integer, YarnApplicationResources> queryResult =
                (Map<Integer, YarnApplicationResources>) queryCont.handle();

        Assert.assertEquals("After remove - records should be zero: ", 0, queryResult.size());

    }

    @Test
    public void testApplicationResource() throws IOException{

        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                YarnApplicationResourcesDataAccess yarnDataAccess = (YarnApplicationResourcesDataAccess)
                        storageFactory.getDataAccess(YarnApplicationResourcesDataAccess.class);

                Assert.assertNotNull("AppResources List should not be null", appResources);
                yarnDataAccess.addAll(appResources);

                connector.commit();

                return null;
            }
        };

        populate.handle();

        LightWeightRequestHandler queryCont = new QueryAppResources(YARNOperationType.TEST);

        Map<String, YarnApplicationResources> queryResult =
                (Map<String, YarnApplicationResources>) queryCont.handle();

        YarnApplicationResourcesDataAccess pr = (YarnApplicationResourcesDataAccess)
                storageFactory.getDataAccess(YarnApplicationResourcesDataAccess.class);

        YarnApplicationResources theApp = pr.findById("app_id_1");
        Assert.assertEquals("Name not equal: ", "app_id_1" ,theApp.getAppId());
        Assert.assertEquals("Memory for app_id_1 not equal: ", 1024, theApp.getAllocated_mb());
        Assert.assertEquals("Vcores for app_id_1 not equal: ", 1, theApp.getAllocated_vcores());
    }

    @Test
    public void TestUpdate() throws IOException {
        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                YarnApplicationResourcesDataAccess yarnDataAccess = (YarnApplicationResourcesDataAccess)
                        storageFactory.getDataAccess(YarnApplicationResourcesDataAccess.class);

                yarnDataAccess.update("app_id_1", 1024, 1);
                connector.commit();

                return null;
            }
        };

        populate.handle();

        YarnApplicationResourcesDataAccess pr = (YarnApplicationResourcesDataAccess)
                storageFactory.getDataAccess(YarnApplicationResourcesDataAccess.class);

        YarnApplicationResources theApp = pr.findById("app_id_1");
        Assert.assertEquals("After Update - Memory for app_id_1 not equal: ", 2048, theApp.getAllocated_mb());
        Assert.assertEquals("After Update - Vcores for app_id_1 not equal: ", 2, theApp.getAllocated_vcores());


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

            Map<Integer, YarnApplicationResources> result = prContDAO.getAll();

            connector.commit();

            return result;
            }
    }

}

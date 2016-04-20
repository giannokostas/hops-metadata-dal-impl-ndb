package io.hops.metadata.ndb.dalimpl.yarn;


import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.YarnApplicationResourcesDataAccess;
import io.hops.metadata.yarn.entity.YarnApplicationResources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class YarnApplicationResourcesClusterJ
        implements TablesDef.YarnApplicationResourcesTableDef,
        YarnApplicationResourcesDataAccess<YarnApplicationResources> {

        private static final Log LOG =
                LogFactory.getLog(YarnApplicationResourcesClusterJ.class);

        @PersistenceCapable(table = TABLE_NAME)
        public interface YarnApplicationResourcesDTO{

                @PrimaryKey
                @Column(name = APP_ID)
                String getAppId();

                void setAppId(String appId);

                @Column(name = ALLOCATED_MB)
                int getallocatedmb();

                void setallocatedmb(int allocatedmb);

                @Column(name = ALLOCATED_VCORES)
                int getallocatedvcores();

                void setallocatedvcores(int allocatedvcores);

        }

        private final ClusterjConnector connector = ClusterjConnector.getInstance();

        @Override
        public YarnApplicationResources findById(String appId) throws StorageException {
                        HopsSession session = connector.obtainSession();

                YarnApplicationResourcesDTO appDTO = session.find(YarnApplicationResourcesDTO.class,
                        appId);

                        if (appDTO != null) {
                                YarnApplicationResources result = create(appDTO);
                            session.release(appDTO);
                            return result;
                        }
                return null;
        }

        @Override
        public void update(String applicationId, int addMemory, int addVcores) throws StorageException{
                YarnApplicationResources appToBeAdded;
                if(findById(applicationId)!= null) {
                        Map<String, YarnApplicationResources> appToBeRemoved = new HashMap<String, YarnApplicationResources>();
                        YarnApplicationResources appForUpdate = findById(applicationId);
                        appToBeRemoved.put(appForUpdate.getAppId(), appForUpdate);
                        removeAll(appToBeRemoved);
                        appToBeAdded = new YarnApplicationResources(appForUpdate.getAppId(),
                                appForUpdate.getAllocated_mb() + addMemory,
                                appForUpdate.getAllocated_vcores() + addVcores);
                }
                else{
                        appToBeAdded = new YarnApplicationResources(applicationId, addMemory, addVcores);
                }
                add(appToBeAdded);
        }


        @Override
        public Map<String, YarnApplicationResources> getAll() throws StorageException {
                HopsSession session = connector.obtainSession();
                HopsQueryBuilder queryBuilder = session.getQueryBuilder();
                HopsQueryDomainType<YarnApplicationResourcesDTO> dto =
                        queryBuilder.createQueryDefinition(YarnApplicationResourcesDTO.class);
                HopsQuery<YarnApplicationResourcesDTO> query =
                        session.createQuery(dto);
                List<YarnApplicationResourcesDTO> resultSet = query.getResultList();

                Map<String, YarnApplicationResources> resultMap = createMap(resultSet);
                session.release(resultSet);

                return resultMap;
        }

        @Override
        public void removeAll(Map<String,YarnApplicationResources> killed) throws StorageException {
                HopsSession session = connector.obtainSession();
                List<YarnApplicationResourcesDTO> toBeRemoved =
                        new ArrayList<YarnApplicationResourcesDTO>();
                for (YarnApplicationResources app : killed.values()) {
                        toBeRemoved.add(session.newInstance(YarnApplicationResourcesDTO.class,
                                app.getAppId()));
                }

                if (!toBeRemoved.isEmpty()) {
                        session.deletePersistentAll(toBeRemoved);
                        session.release(toBeRemoved);
                }
        }

        @Override
        public void add(YarnApplicationResources yarnApplicationResources) throws StorageException {
                HopsSession session = connector.obtainSession();
                YarnApplicationResourcesClusterJ.YarnApplicationResourcesDTO toAdd = createPersistable(
                        yarnApplicationResources, session);
                session.savePersistent(toAdd);
                session.release(toAdd);
        }

        @Override
        public void addAll(Map<String, YarnApplicationResources> toBeAdded) throws StorageException{

                for (Map.Entry<String, YarnApplicationResources> entry : toBeAdded.entrySet()) {
                        this.add(entry.getValue());
                }
        }


        private YarnApplicationResourcesClusterJ.YarnApplicationResourcesDTO createPersistable(
                YarnApplicationResources yarnRes,
                HopsSession session) throws StorageException {
                YarnApplicationResourcesClusterJ.YarnApplicationResourcesDTO appResDTO = session.newInstance(
                        YarnApplicationResourcesClusterJ.YarnApplicationResourcesDTO.class);
                appResDTO.setAppId(yarnRes.getAppId());
                appResDTO.setallocatedmb(yarnRes.getAllocated_mb());
                appResDTO.setallocatedvcores(yarnRes.getAllocated_vcores());

                return appResDTO;
        }

        private Map<String, YarnApplicationResources> createMap(List<YarnApplicationResourcesDTO> resultSet) {
                Map<String, YarnApplicationResources> resultMap = new HashMap<String, YarnApplicationResources>();
                YarnApplicationResources yarnApp;

                for (YarnApplicationResourcesDTO dto : resultSet) {
                        yarnApp = create(dto);
                        resultMap.put(dto.getAppId(), yarnApp);
                }

                return resultMap;
        }

        private YarnApplicationResources create(YarnApplicationResourcesDTO dto) {
                return new YarnApplicationResources(dto.getAppId(), dto.getallocatedmb(), dto.getallocatedvcores());
        }
}

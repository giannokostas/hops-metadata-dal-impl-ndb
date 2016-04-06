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
                @Column(name = INODE_ID)
                int getinodeid();

                void setinodeid(int inodeid);

                @Column(name = NAME)
                String getname();

                void setname(String name);

                @Column(name = ALLOCATED_MB)
                int getallocatedmb();

                void setallocatedmb(int allocatedmb);

                @Column(name = ALLOCATED_VCORES)
                int getallocatedvcores();

                void setallocatedvcores(int allocatedvcores);

        }

        private final ClusterjConnector connector = ClusterjConnector.getInstance();

        @Override
        public YarnApplicationResources findById(int inodeID) throws StorageException {
                        HopsSession session = connector.obtainSession();

                YarnApplicationResourcesDTO appDTO = session.find(YarnApplicationResourcesDTO.class,
                        inodeID);

                        if (appDTO != null) {
                                YarnApplicationResources result = create(appDTO);
                            session.release(appDTO);
                            return result;
                        }
                return null;
        }


        @Override
        public Map<Integer, YarnApplicationResources> getAll() throws StorageException {
                HopsSession session = connector.obtainSession();
                HopsQueryBuilder queryBuilder = session.getQueryBuilder();
                HopsQueryDomainType<YarnApplicationResourcesDTO> dto =
                        queryBuilder.createQueryDefinition(YarnApplicationResourcesDTO.class);
                HopsQuery<YarnApplicationResourcesDTO> query =
                        session.createQuery(dto);
                List<YarnApplicationResourcesDTO> resultSet = query.getResultList();

                Map<Integer, YarnApplicationResources> resultMap = createMap(resultSet);
                session.release(resultSet);

                return resultMap;
        }

        @Override
        public void removeAll(Collection<YarnApplicationResources> killed)
                throws StorageException {
                HopsSession session = connector.obtainSession();
                List<YarnApplicationResourcesDTO> toBeRemoved =
                        new ArrayList<YarnApplicationResourcesDTO>();
                for (YarnApplicationResources cont : killed) {
                        toBeRemoved.add(createPersistable(cont, session));
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

        private YarnApplicationResourcesClusterJ.YarnApplicationResourcesDTO createPersistable(
                YarnApplicationResources yarnRes,
                HopsSession session) throws StorageException {
                YarnApplicationResourcesClusterJ.YarnApplicationResourcesDTO appResDTO = session.newInstance(
                        YarnApplicationResourcesClusterJ.YarnApplicationResourcesDTO.class);
                //Set values to persist new YarnHistoryPriceDTO
                appResDTO.setinodeid(yarnRes.getInode_id());
                appResDTO.setname(yarnRes.getName());
                appResDTO.setallocatedmb(yarnRes.getAllocated_mb());
                appResDTO.setallocatedvcores(yarnRes.getAllocated_vcores());

                return appResDTO;
        }

        private Map<Integer, YarnApplicationResources> createMap(List<YarnApplicationResourcesDTO> resultSet) {
                Map<Integer, YarnApplicationResources> resultMap = new HashMap<Integer, YarnApplicationResources>();
                YarnApplicationResources yarnApp;

                for (YarnApplicationResourcesDTO dto : resultSet) {
                        yarnApp = create(dto);
                        resultMap.put(dto.getinodeid(), yarnApp);
                }

                return resultMap;
        }

        private YarnApplicationResources create(YarnApplicationResourcesDTO dto) {
                return new YarnApplicationResources(dto.getinodeid(),
                        dto.getname(), dto.getallocatedmb(), dto.getallocatedvcores());
        }
}

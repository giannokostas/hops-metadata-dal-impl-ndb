package io.hops.metadata.ndb.dalimpl.yarn;


import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.YarnApplicationResourcesDataAccess;
import io.hops.metadata.yarn.entity.YarnApplicationResources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
                        YarnApplicationResourcesClusterJ. YarnApplicationResourcesDTO.class);
                //Set values to persist new YarnHistoryPriceDTO
                appResDTO.setinodeid(yarnRes.getInode_id());
                appResDTO.setname(yarnRes.getName());
                appResDTO.setallocatedmb(yarnRes.getAllocated_mb());
                appResDTO.setallocatedvcores(yarnRes.getAllocated_vcores());

                return appResDTO;

        }
}

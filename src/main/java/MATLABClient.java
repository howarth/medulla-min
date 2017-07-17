import main.scala.store.*;
import main.scala.core.*;

public class MATLABClient {

    private String basePath;
    private AlphaContext alphaContext;
    private BinRegistry registry;

    public MATLABClient(String basePath){
        this.basePath = basePath;
        this.registry = new BinRegistry(basePath);
        this.alphaContext = new AlphaContext(this.registry, new BinStore(basePath,50));
    }

    public Object[] get(String[] identifiers){

        int nids = identifiers.length;
        Object[] returnArr = new Object[nids];

        for(int i = 0; i < nids; i++){
            String idStr = identifiers[i];
            DataId did = this.registry.getTypedId(idStr);
            String dataTypeString = DataTypeStringData.typeStringFromId(did);
            Data returnedData = this.alphaContext.get(did);
            if (did instanceof ScalarId) {
                returnArr[i] = new Object[]{idStr, dataTypeString, returnedData.data()};
            } else if (did instanceof main.scala.core.MultiChannelTimeSeriesId){
                //returnArr[i] = {idStr, this.dtsd.typeStringFromId(did), returnedData.data().data(), returnedData.};
            } else if (did instanceof main.scala.core.VectorId) {
                VectorData tmpD = (VectorData) returnedData;
                returnArr[i] = new Object[]{idStr, dataTypeString, tmpD.data().data()};
            }else if (did instanceof main.scala.core.Matrix2DId) {
                Matrix2DData tmpD = (Matrix2DData) returnedData;
                returnArr[i] = new Object[]{idStr, dataTypeString, tmpD.data().data()};
            }
        }
        return returnArr;
    }
}

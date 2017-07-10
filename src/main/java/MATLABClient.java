import main.scala.store.BinStore;
import main.scala.core.DoubleMultiChannelTimeSeriesId;

public class MATLABClient {

    private String basePath;

    public MATLABClient(String basePath){
        this.basePath = basePath;
    }

    public Object[] get(String[] s){
        double[] a =  {1.0, 2.0};
        String[] b = {"3.0", "4.0", "5.0"};

        BinStore store = new BinStore(this.basePath, 32);
        DoubleMultiChannelTimeSeriesId tsId = new DoubleMultiChannelTimeSeriesId("test:A:01:raw");
        Object[] c = {a, b, this.basePath,
                store.getMultiChannelTimeSeries(tsId).data().data()};

        return c;
    }

    public static void main(String[] args){
        MATLABClient m = new MATLABClient("/Users/dhowarth/work/db/bindata");
    }
}

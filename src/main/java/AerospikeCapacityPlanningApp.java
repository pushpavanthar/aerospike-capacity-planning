import com.aerospike.client.*;
import com.aerospike.client.cdt.ListOperation;

import java.io.IOException;
import java.util.*;

/**
 * Created by purushotham on 07/03/18.
 */

public class AerospikeCapacityPlanningApp {
    private static AerospikeConnectionManager connectionManager;

    public static void main(String[] args) throws Exception {
        int retCode = -1;
        if (args.length < 2) {
            System.exit(retCode);
        }
        connectionManager = new AerospikeConnectionManager(args[0], args[1]);
        connectionManager.start();
        AerospikeCapacityPlanningApp app = new AerospikeCapacityPlanningApp();
        try {
            app.execute(args[2]);
        } catch (Exception e) {
            e.printStackTrace();
            app.get();
            connectionManager.stop();
            System.exit(retCode);
        }
        app.get();
        connectionManager.stop();
        System.exit(retCode);
    }

    public void get() throws Exception {
        Key key = new Key("test", "activity", "user_123123");
        AerospikeClient client = connectionManager.getClient();
        Record record = client.get(null, key);
        List<byte[]> blob = (ArrayList<byte[]>) record.bins.get("2018-02-12");
        for(byte[] bytes : blob) {
            System.out.println(Gzip.decompress(bytes));
        }

    }

    public void execute(String numberOfRecords) throws Exception {
        int recordCount = Integer.valueOf(numberOfRecords);
        AerospikeClient client = connectionManager.getClient();
        Random random = new Random();
        RandomString randomString = new RandomString(50, random);

        List<Value> documentList = new ArrayList<Value>();

        documentList.add(populateDocument(random, randomString));
        Key key = new Key("test", "activity", "user_123123");
        Bin bin = new Bin("2018-02-12", documentList);
        client.put(client.writePolicyDefault, key, bin);
        documentList.clear();
        for(int i = 1 ; i <= recordCount; i++) {

            documentList.add(populateDocument(random, randomString));
            if(i%1 == 0) {
                System.out.println("record number : " + i);
                client.operate(client.writePolicyDefault, key, ListOperation.appendItems(bin.name, documentList));
                documentList.clear();
            }

        }


    }

    private Value populateDocument(Random random, RandomString randomString) throws IOException{

        Map<String, Object> document = new HashMap();
        document.put("attribute_1", random.nextInt(10000000));
        document.put("attribute_2", random.nextInt(10000000));
        document.put("attribute_3", random.nextInt(10000000));
        document.put("attribute_4", random.nextInt(10000000));
        document.put("attribute_5", random.nextInt(10000000));
        document.put("attribute_6", random.nextInt(10000000));
        document.put("attribute_7", randomString.nextString());
        document.put("attribute_8", randomString.nextString());
        document.put("attribute_9", randomString.nextString());
        document.put("attribute_10",randomString.nextString());
        document.put("attribute_11",randomString.nextString());
        document.put("attribute_12",randomString.nextString());
        document.put("attribute_13",randomString.nextString());
        document.put("attribute_14",randomString.nextString());
        document.put("attribute_15",randomString.nextString());
        document.put("attribute_16",randomString.nextString());
        document.put("attribute_17",randomString.nextString());
        document.put("attribute_18",randomString.nextString());
        document.put("attribute_19",randomString.nextString());
        document.put("attribute_20", random.nextInt(10000000));
        return Value.get(Gzip.compress(document.toString()));
    }
}

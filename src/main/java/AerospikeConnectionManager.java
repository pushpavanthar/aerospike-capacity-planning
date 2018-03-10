import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.policy.ClientPolicy;
import lombok.Data;

/**
 * Created by purushotham on 07/03/18.
 */


@Data
public class AerospikeConnectionManager {

    private String aerospikeHosts;
    private AerospikeClient client;
    private final Monitor monitor = new Monitor();
    private final int writeTimeout = 5000;
    private static final int MAX_RETRIES = 3;
    private final Integer TTL;

    /**
     * Constructor
     *
     */
    public AerospikeConnectionManager(String hosts, String ttl) {
        aerospikeHosts = hosts;
        TTL = Integer.valueOf(ttl);
    }

    public void start() throws Exception {
        final Host[] hostArray = getHost(aerospikeHosts);
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.writePolicyDefault.setTimeout(writeTimeout);
        clientPolicy.writePolicyDefault.sendKey = true;
        clientPolicy.writePolicyDefault.maxRetries = MAX_RETRIES;
        clientPolicy.writePolicyDefault.expiration = TTL;

        client = new AerospikeClient(clientPolicy, hostArray);
    }

    public void stop() throws Exception {
        client.close();
    }

    public static Host[] getHost(String hosts) {
        String [] hostColl = hosts.split(",");
        Host[] hostArr = new Host[hostColl.length];
        int index = 0;
        for(String host: hostColl){
            String hostParts[] = host.split(":");
            hostArr[index] = new Host(hostParts[0],Integer.parseInt(hostParts[1]));
            index++;
        }
        return hostArr;
    }
}

package dlt.load.balancer.model;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.util.stream.Collectors.toList;
import org.json.JSONArray;
import org.json.JSONException;

/**
 *
 * @author Uellington Damasceno
 */
public class BalancerConfigs {

    private final Boolean balanceable;
    private final Boolean multiLayerBalancing;
    private final String mqttPort;

    private final Long TIMEOUT_LB_REPLY;
    private final Long TIMEOUT_GATEWAY;
    private final Long LB_ENTRY_TIMEOUT;

    private final Long maxQtyConnectedDevices;
    private final Long maxTryResendTransaction;

    private final Long maxPublishMessageInterval;

    private final List<String> interestedTopics;

    private static final Logger logger = Logger.getLogger(BalancerConfigs.class.getName());

    public BalancerConfigs() {
        this.balanceable = this.readBalanceableEnv();
        this.multiLayerBalancing = this.readMultiBalanceableEnv();
        this.mqttPort = this.readMqttPortEnv();
        this.maxPublishMessageInterval = this.readMaxPublishMessageIntervalEnv();
        this.interestedTopics = this.readInterestedTopics();
        this.TIMEOUT_GATEWAY = this.readTimeoutGateway();
        this.TIMEOUT_LB_REPLY = this.readTimeoutLBReply();
        this.LB_ENTRY_TIMEOUT = this.readTimeoutLBEntry();
        this.maxTryResendTransaction = this.readMaxTryResendTransaction();
        this.maxQtyConnectedDevices = this.readLoadLimit();
    }

    public Long getLBStartReplyTimeout() {
        return TIMEOUT_LB_REPLY;
    }

    public Long getTimeoutGateway() {
        return TIMEOUT_GATEWAY;
    }
    
    public Long getLBEntryResponseTimeout(){
        return LB_ENTRY_TIMEOUT;
    }
    

    public Long getLoadLimit() {
        return maxQtyConnectedDevices;
    }

    public Boolean isBalanceable() {
        return balanceable;
    }

    public Boolean isMultiLayerBalancing() {
        return multiLayerBalancing;
    }

    public String getMqttPort() {
        return mqttPort;
    }

    public List<String> getInterestedTopics() {
        return interestedTopics;
    }

    public boolean validPublishMessageInterval(Long publishTime) {
        return (System.currentTimeMillis() - publishTime) < maxPublishMessageInterval;
    }

    public Long getMaxTryResendTransaction() {
        return maxTryResendTransaction;
    }

    private Boolean readBalanceableEnv() {
        String isBalanceableValue = System.getenv("IS_BALANCEABLE");
        if (isBalanceableValue == null) {
            logger.warning("Load balancer configs - IS_BALANCEABLE env var is not defined.");
            return true;
        }
        return isBalanceableValue.equals("1");
    }

    private Boolean readMultiBalanceableEnv() {
        String isMultiLayer = System.getenv("IS_MULTI_LAYER");
        if (isMultiLayer == null) {
            logger.warning("Load balancer configs - IS_MULTI_LAYER env var is not defined.");
            return true;
        }
        return isMultiLayer.equals("1");
    }

    private String readMqttPortEnv() {
        String port = System.getenv("GATEWAY_PORT");
        if (port == null) {
            logger.warning("Load balancer configs - GATEWAY_PORT env is not defined");
            return "1883";
        }
        return port;
    }

    private Long readMaxPublishMessageIntervalEnv() {
        return readEnvLongOrDefault("VALID_MESSAGE_INTERVAL", 15000L);
    }

    private Long readTimeoutGateway() {
        return readEnvLongOrDefault("TIMEOUT_GATEWAY", 40000L);
    }

    private Long readTimeoutLBReply() {
        return readEnvLongOrDefault("TIMEOUT_LB_REPLY", 20000L);
    }

    private Long readTimeoutLBEntry() {
        return readEnvLongOrDefault("LB_ENTRY_TIMEOUT", 20000L);
    }

    private Long readMaxTryResendTransaction() {
        return this.readEnvLongOrDefault("MAX_TRY_RESEND_TRANS", 3L);
    }

    private Long readLoadLimit() {
        return this.readEnvLongOrDefault("LOAD_LIMIT", 10L);
    }

    private Long readEnvLongOrDefault(String envName, Long defaultValue) {
        String readEnv = System.getenv(envName);
        if (readEnv == null) {
            logger.log(Level.WARNING, "Load balancer configs - {0} env is not defined", envName);
            return defaultValue;
        }

        try {
            return Long.valueOf(readEnv);
        } catch (NumberFormatException nfe) {
            return defaultValue;
        }
    }

    private List<String> readInterestedTopics() {
        String envTopics = System.getenv("TOPICS");
        if (envTopics != null) {
            try {
                JSONArray array = new JSONArray(envTopics);
                return array.toList().stream()
                        .filter(String.class::isInstance)
                        .map(String.class::cast)
                        .peek(topic -> logger.log(Level.INFO, "Topic read: {0}", topic))
                        .collect(toList());
            } catch (JSONException e) {
                logger.log(Level.SEVERE, "Formato inválido para TOPICS: " + envTopics, e);
            }
        }
        return this.defaultTopicsList();
    }

    private List<String> defaultTopicsList() {
        return List.of(
                "LB_ENTRY",
                "LB_ENTRY_REPLY",
                "LB_STATUS",
                "LB_REQUEST",
                "LB_REPLY",
                "LB_DEVICE",
                "LB_MULTI_REQUEST",
                "LB_MULTI_RESPONSE",
                "LB_MULTI_DEVICE_REQUEST",
                "LB_MULTI_DEVICE_RESPONSE"
        );
    }

    @Override
    public String toString() {
        return "BalancerConfigs{" + "balanceable=" + balanceable + 
                ", multiLayerBalancing=" + multiLayerBalancing + 
                ", mqttPort=" + mqttPort + 
                ", TIMEOUT_LB_REPLY=" + TIMEOUT_LB_REPLY + 
                ", TIMEOUT_GATEWAY=" + TIMEOUT_GATEWAY + 
                ", LB_ENTRY_TIMEOUT=" + LB_ENTRY_TIMEOUT + 
                ", maxQtyConnectedDevices=" + maxQtyConnectedDevices + 
                ", maxTryResendTransaction=" + maxTryResendTransaction + 
                ", maxPublishMessageInterval=" + maxPublishMessageInterval + '}';
    }



}

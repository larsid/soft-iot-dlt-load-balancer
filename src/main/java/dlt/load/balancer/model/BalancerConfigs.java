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
    private final Boolean displayPastTimeTransPub;
    private final String mqttPort;

    private final Long TIMEOUT_LB_SINGLE_REPLY, TIMEOUT_LB_MULTI_REPLY;
    private final Long TIMEOUT_GATEWAY;
    private final Long LB_ENTRY_TIMEOUT;

    private final Long maxQtyConnectedDevices;
    private final Long maxTryResendTransaction;

    private final Long maxPublishMessageInterval;

    private final List<String> interestedTopics;

    private static final Logger logger = Logger.getLogger(BalancerConfigs.class.getName());

    private static BalancerConfigs instance;
    
    private BalancerConfigs() {
        this.balanceable = this.readBalanceableEnv();
        this.multiLayerBalancing = this.readMultiBalanceableEnv();
        this.mqttPort = this.readMqttPortEnv();
        this.maxPublishMessageInterval = this.readMaxPublishMessageIntervalEnv();
        this.interestedTopics = this.readInterestedTopics();
        this.TIMEOUT_GATEWAY = this.readTimeoutGateway();
        this.TIMEOUT_LB_SINGLE_REPLY = this.readTimeoutLBSingleReply();
        this.TIMEOUT_LB_MULTI_REPLY = this.readTimeoutLBMultiReply();
        this.LB_ENTRY_TIMEOUT = this.readTimeoutLBEntry();
        this.maxTryResendTransaction = this.readMaxTryResendTransaction();
        this.maxQtyConnectedDevices = this.readLoadLimit();
        this.displayPastTimeTransPub = this.readShouldDisplayPastTime();
    }
    
    public static synchronized BalancerConfigs getInstance(){
        if(instance == null){
            instance = new BalancerConfigs();
        }
        return instance;
    }

    public Long getLBSingleStartReplyTimeout() {
        return TIMEOUT_LB_SINGLE_REPLY;
    }

    public Long getLBMultiStartReplyTimeout() {
        return TIMEOUT_LB_MULTI_REPLY;
    }

    public Long getimeoutGateway() {
        return TIMEOUT_GATEWAY;
    }

    public Long getLBEntryResponseTimeout() {
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

    public boolean shouldDisplayPastTimeTransPub() {
        return this.displayPastTimeTransPub;
    }

    private Boolean readShouldDisplayPastTime() {
        return this.readEnvBooleanOrDefault("DISPLAY_PAST_TRANS_TIME", true);
    }

    private Boolean readBalanceableEnv() {
        return this.readEnvBooleanOrDefault("IS_BALANCEABLE", true);
    }

    private Boolean readMultiBalanceableEnv() {
        return this.readEnvBooleanOrDefault("IS_MULTI_LAYER", true);
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

    private Long readTimeoutLBSingleReply() {
        return readEnvLongOrDefault("TIMEOUT_LB_SINGLE_REPLY", 20000L);
    }
    
    private Long readTimeoutLBMultiReply(){
        return readEnvLongOrDefault("TIMEOUT_LB_MULTI_REPLY", 20000L);
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

    private boolean readEnvBooleanOrDefault(String envName, Boolean defaultValue) {
        String isMultiLayer = System.getenv(envName);
        if (isMultiLayer == null) {
            logger.log(Level.WARNING, "Load balancer configs - {0} env var is not defined.", envName);
            return defaultValue;
        }
        return isMultiLayer.equals("1");
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
        return "BalancerConfigs{" + "balanceable=" + balanceable
                + ", multiLayerBalancing=" + multiLayerBalancing
                + ", mqttPort=" + mqttPort
                + ", TIMEOUT_LB_REPLY=" + TIMEOUT_LB_SINGLE_REPLY
                + ", TIMEOUT_GATEWAY=" + TIMEOUT_GATEWAY
                + ", LB_ENTRY_TIMEOUT=" + LB_ENTRY_TIMEOUT
                + ", maxQtyConnectedDevices=" + maxQtyConnectedDevices
                + ", maxTryResendTransaction=" + maxTryResendTransaction
                + ", maxPublishMessageInterval=" + maxPublishMessageInterval + '}';
    }

}

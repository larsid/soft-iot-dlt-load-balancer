package dlt.load.balancer.model;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.MqttException;

import com.google.gson.JsonObject;

import br.uefs.larsid.extended.mapping.devices.services.IDevicePropertiesManager;
import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import dlt.auth.services.IPublisher;
import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.LBMultiRequest;
import dlt.client.tangle.hornet.model.transactions.Status;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import dlt.client.tangle.hornet.services.ILedgerSubscriber;
import dlt.id.manager.services.IDLTGroupManager;
import dlt.id.manager.services.IIDManagerService;

/**
 *
 * @author Allan Capistrano, Antonio Crispim, Uellington Damasceno
 * @version 1.2.4
 */
public class Balancer implements ILedgerSubscriber, Runnable {

    private ScheduledExecutorService scheduler;

    private LedgerConnector connector;

    private IDevicePropertiesManager deviceManager;
    private IIDManagerService idManager;
    private IDLTGroupManager groupManager;
    private IPublisher iPublisher;

    private boolean isSubscribed;
    private String gatewayId;

    private BalancerConfigs configs;

    private static final Logger logger = Logger.getLogger(Balancer.class.getName());

    private BalancerStateManager stateManager;

    private int messageSingleLayerSentCounter;
    private int messageMultiLayerSentCounter;
    private final int MAX_ATTEMPTS_SEND_START_BALANCE;

    public Balancer() {
        this.isSubscribed = false;
        this.messageSingleLayerSentCounter = 0;
        this.messageMultiLayerSentCounter = 0;
        this.MAX_ATTEMPTS_SEND_START_BALANCE = 3;
    }

    private ScheduledExecutorService buildScheduledExecutorService() {
        return Executors.newSingleThreadScheduledExecutor((Runnable r) -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("BalancerTimeoutScheduler");
            return t;
        });
    }

    public void setPublisher(IPublisher iPublisher) {
        this.iPublisher = iPublisher;
    }

    public void setDeviceManager(IDevicePropertiesManager deviceManager) {
        this.deviceManager = deviceManager;
    }

    public void setConnector(LedgerConnector connector) {
        this.connector = connector;
    }

    public void setIdManager(IIDManagerService idManager) {
        this.idManager = idManager;
    }

    public void setGroupManager(IDLTGroupManager groupManager) {
        this.groupManager = groupManager;
    }

    public void start() {
        this.configs = BalancerConfigs.getInstance();
        logger.info(this.configs.toString());

        this.gatewayId = new StringBuilder(this.groupManager.getGroup())
                .append("/")
                .append(this.idManager.getIP())
                .append("/")
                .append(this.configs.getMqttPort())
                .toString();

        this.scheduler = this.buildScheduledExecutorService();
        this.scheduler.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS);
        this.stateManager = new BalancerStateManager(this);
    }

    public void stop() {
        this.isSubscribed = false;

        this.configs
                .getInterestedTopics()
                .forEach(topic -> this.connector.unsubscribe(topic, this));

        this.scheduler.shutdown();

        try {
            if (this.scheduler.awaitTermination(500, TimeUnit.MILLISECONDS)
                    && !this.scheduler.isShutdown()) {
                this.scheduler.shutdownNow();
            }
        } catch (InterruptedException ex) {
            this.scheduler.shutdownNow();
        }
    }

    protected boolean sendTransaction(Transaction transaction) {
        try {
            this.connector.put(transaction);
            return true;
        } catch (InterruptedException ex) {
            logger.info("Load Balancer - Error commit transaction.");
            Logger.getLogger(Balancer.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    public void sendDevice(Device device, String targetIp, String targetMqttPort) throws MqttException {
        try {

            JsonObject jsonPublish = new JsonObject();
            jsonPublish.addProperty("id", device.getId());
            jsonPublish.addProperty("url", "tcp://" + targetIp);
            jsonPublish.addProperty("port", targetMqttPort);
            jsonPublish.addProperty("user", "karaf");
            jsonPublish.addProperty("password", "karaf");

            String topic = "dev/" + device.getId();

            iPublisher.publish(
                    topic,
                    "SET VALUE brokerMqtt" + jsonPublish.toString()
            );

            deviceManager.removeDevice(device.getId());

        } catch (IOException ioe) {
            logger.info("Error! To retrieve device list or to remove the first device.");
            logger.log(Level.WARNING, "Exception occurred while sending device.", ioe);
        }
    }

    public void receiveNewDevice(String deviceJSON) {
        try {
            Device device = DeviceWrapper.toDevice(deviceJSON);
            logger.log(Level.INFO, "Device after convert: {0}", DeviceWrapper.toJSON(device));

            deviceManager.addDevice(device);
        } catch (IOException ioe) {
            logger.info("Error! To add a new device to the list.");
            logger.log(Level.WARNING, "Exception occurred while adding a new device.", ioe);
        }
    }

    protected Long qtyMaxTimeResendTransaction() {
        return this.configs.getMaxTryResendTransaction();
    }

    protected Long getLBStartReplyTimeWaiting() {
        return this.isMultiLayerBalancer()
                ? this.configs.getLBSingleStartReplyTimeout()
                : this.configs.getLBMultiStartReplyTimeout();
    }

    protected Long getLBDeviceRecivedReplyTimeWaiting() {
        return this.configs.getLBSingleStartReplyTimeout() * 2;
    }

    protected Long getLBRequestTimeWaiting() {
        return this.configs.getLBSingleStartReplyTimeout() * 2;
    }

    protected boolean shouldDisplayPastTimeTransPublication() {
        return this.configs.shouldDisplayPastTimeTransPub();
    }

    protected String getGatewayGroup() {
        return this.groupManager.getGroup();
    }

    protected boolean isMultiLayerBalancer() {
        return this.configs.isMultiLayerBalancing();
    }

    protected boolean isValidPublishMessageInterval(long publishedAt) {
        return this.configs.validPublishMessageInterval(publishedAt);
    }

    public void transitionTo(String gatewayTarget, AbstractBalancerState newState) {
        this.stateManager.transitionTo(gatewayTarget, newState);
    }

    public void transiteOverloadedStateTo(AbstractBalancerState newState, boolean initializeState) {
        this.stateManager.transiteOverloadedStateTo(newState, initializeState);
    }

    public ScheduledFuture<?> scheduleTimeout(BalancerState state, long delayMillis) {
        return scheduler.schedule(state::onTimeout, delayMillis, TimeUnit.MILLISECONDS);
    }

    protected Optional<Device> getFirstDevice() throws IOException {
        List<Device> devices = this.deviceManager.getAllDevices();
        if (devices.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(devices.get(0));
    }

    @Override
    public void update(Object trans, Object messageId) {

        if (!this.configs.isBalanceable()) {
            logger.log(Level.INFO, "Load balancer - New message but will not processed because is not balanceable.");
            return;
        }

        if (!(trans instanceof Transaction)) {
            logger.log(Level.INFO, "Load balancer - New message with id: {0} is Invalid", messageId);
            return;
        }

        Transaction transaction = (Transaction) trans;

        if (!this.isValidPublishMessageInterval(transaction.getPublishedAt())) {
            logger.log(Level.WARNING, "{0} foi é considerada antiga.", transaction);
            return;
        }

        if (transaction.isMultiLayerTransaction() && !this.isMultiLayerBalancer()) {
            logger.info("Load balancer - Multilayer message type not allowed.");
            return;
        }

        boolean isLoopback = transaction.isLoopback(gatewayId);

        if (transaction.is(TransactionType.LB_STATUS)) {
            if (!isLoopback) {
                return;
            }
            Status internalStatus = (Status) transaction;
            this.stateManager.updateInternalStatus(internalStatus);
            if (this.isGatewayBalanced(internalStatus.getLastLoad())) {
                logger.info("Gateway already balanced or free to recive new device.");
                return;
            }
            this.startBalancingRequest(internalStatus, this.gatewayId);
            return;
        }

        if (this.isTransactionToStartBalancing(transaction)) {
            if (!this.stateManager.canReciveNewDevice(this.configs.getLoadLimit())) {
                logger.info("This gateway is not avaliable to recive new devices.");
                return;
            }
            String overloadedGateway = transaction.getSource();
            
            this.stateManager.addBalancerRequestHandle(overloadedGateway, new IdleState(this));
        }

        Optional<BalancerState> optState;

        optState = this.stateManager.getBalancerByTransaction(transaction);

        if (optState.isEmpty()) {
            logger.log(Level.WARNING,"there not definied state to handler this transaction: {0}", transaction.toString());
            return;
        }
        
        BalancerState state = optState.get();
        
        if (isLoopback && !state.canProcessLoopback(transaction)) {
            logger.log(Level.INFO, "The current state can not handle loopback message: {0}", transaction.toString());
            return;
        }

        state.handle(transaction, gatewayId);
    }
    public String getGatewayId(){
        return this.gatewayId;
    }
    public boolean isGatewayBalanced(double currentDeviceLoad) {
        double deviceLoadLimit = this.configs.getLoadLimit().doubleValue();
        return currentDeviceLoad <= deviceLoadLimit;
    }

    public void startBalancingRequestWithCurrentInternalStaus() {
        Status status = this.stateManager.currentInternalStatus();
        this.startBalancingRequest(status, gatewayId);
    }

    public void startBalancingRequest(Status transaction, String gatewayId) {

        boolean isMultiLayerBalancer = this.isMultiLayerBalancer();
        double currentDeviceLoad = transaction.getLastLoad();

        if (this.isGatewayBalanced(currentDeviceLoad)) {
            if (isMultiLayerBalancer) {
                this.messageMultiLayerSentCounter = 0;
            } else {
                this.messageSingleLayerSentCounter = 0;
            }
            return;
        }

        if (!this.stateManager.isGatewayOverloadIdleState()) {
            logger.info("The gateway is balancing. Therefore not will be send a new balancing request.");
            return;
        }

        String targetGroup = this.groupManager.getGroup();

        Transaction startBalanceTransactionSignal;

        if (!isMultiLayerBalancer && this.messageSingleLayerSentCounter == MAX_ATTEMPTS_SEND_START_BALANCE) {
            this.messageSingleLayerSentCounter = 0;
        }

        if (this.messageSingleLayerSentCounter < MAX_ATTEMPTS_SEND_START_BALANCE) {
            logger.log(Level.INFO, "Solicitação interna de balanceamento de camada unica nº {0} iniciada.", messageSingleLayerSentCounter);

            startBalanceTransactionSignal = new Status(gatewayId, targetGroup, true, currentDeviceLoad, transaction.getAvgLoad(), false);
            this.startBalancing(new ProcessSingleLayerSendDeviceState(this), startBalanceTransactionSignal);
            this.sendTransaction(startBalanceTransactionSignal);
            this.messageSingleLayerSentCounter++;
            return;
        }

        if (this.messageMultiLayerSentCounter == MAX_ATTEMPTS_SEND_START_BALANCE) {
            this.messageMultiLayerSentCounter = 0;
        }

        if (this.messageMultiLayerSentCounter < MAX_ATTEMPTS_SEND_START_BALANCE) {
            logger.log(Level.INFO, "Solicitação interna de balanceamento de multi camadas nº {0} iniciada.", messageMultiLayerSentCounter);

            startBalanceTransactionSignal = new LBMultiRequest(gatewayId, targetGroup);
            this.startBalancing(new ProcessMultiLayerSendDeviceState(this), startBalanceTransactionSignal);
            this.messageMultiLayerSentCounter++;
        }
    }

    public void startBalancing(AbstractBalancerState nextState, Transaction request) {
        this.stateManager.transiteOverloadedStateTo(nextState, false);
        this.sendTransaction(request);
    }

    private boolean isTransactionToStartBalancing(Transaction transaction) {
        return transaction.is(TransactionType.LB_ENTRY)
                || transaction.is(TransactionType.LB_MULTI_REQUEST);
    }

    /* Thread para verificar o IP do client tangle e se inscrever nos tópicos
    de interesse.*/
    @Override
    public void run() {
        try {
            String ledgerUrl = this.connector.getLedgerWriter().getUrl();

            URL urlObj = new URL(ledgerUrl);
            String host = urlObj.getHost();
            InetAddress inet = InetAddress.getByName(host);

            if (!inet.isReachable(5000)) {
                logger.log(Level.WARNING, "Host inacessível: {0}. flagSubscribe setado para true.", host);
                this.isSubscribed = false;
            }

            if (this.isSubscribed) {
                return;
            }

            this.configs
                    .getInterestedTopics()
                    .forEach(topic -> {
                        logger.log(Level.INFO, "Inscrevendo-se no tópico: {0}", topic);
                        this.connector.subscribe(topic, this);
                    });

            this.isSubscribed = true;
            logger.info("Inscrição concluída. flagSubscribe setado para false.");

        } catch (UnknownHostException uhe) {
            logger.log(Level.SEVERE, "Erro: Host desconhecido.", uhe);
            this.isSubscribed = false;
        } catch (IOException ioe) {
            logger.log(Level.SEVERE, "Erro: Falha ao conectar com o endereço.", ioe);
            this.isSubscribed = false;
        }
    }
}

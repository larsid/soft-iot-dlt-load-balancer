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
    private ScheduledFuture<?> timeoutFuture;

    private LedgerConnector connector;

    private IDevicePropertiesManager deviceManager;
    private IIDManagerService idManager;
    private IDLTGroupManager groupManager;
    private IPublisher iPublisher;

    private Status internalStatus;
    private boolean isSubscribed;

    private final BalancerConfigs configs;
    private static final Logger logger = Logger.getLogger(Balancer.class.getName());

    private Long lastAttemptStartBalanceTime;
    private int messageSingleLayerSentCounter;
    private int messageMultiLayerSentCounter;
    private final int MAX_ATTEMPTS_SEND_START_BALANCE;

    private BalancerState state;

    public Balancer() {
        this.isSubscribed = false;
        this.messageSingleLayerSentCounter = 0;
        this.messageMultiLayerSentCounter = 0;
        this.MAX_ATTEMPTS_SEND_START_BALANCE = 3;
        this.configs = new BalancerConfigs();
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
        logger.info(this.configs.toString());
        this.scheduler = this.buildScheduledExecutorService();
        this.scheduler.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS);
        this.state = new IdleState(this);
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

            iPublisher.publish(
                    device.getId(),
                    "SET VALUE brokerMqtt{" + jsonPublish.toString() + "}"
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
    
    public void updateInternalStatus(Status transaction) {
        this.internalStatus = transaction;
        double currentDeviceCount = transaction.getLastLoad();
        Long maxDeviceCount = this.configs.getLoadLimit();
        boolean isMultiLayerBalancer = this.isMultiLayerBalancer();

        if (currentDeviceCount < maxDeviceCount || this.state.isBalancing()) {
            if (isMultiLayerBalancer) {
                this.messageMultiLayerSentCounter = 0;
                return;
            }
            this.messageSingleLayerSentCounter = 0;
        }
        
        String sourceIdentifier = this.buildSource();
        String targetGroup = this.groupManager.getGroup();
        Long balanceAttemptIntervalMillis = this.configs.getLBEntryResponseTimeout();

        if (lastAttemptStartBalanceTime == null) {
            this.lastAttemptStartBalanceTime = System.currentTimeMillis();
        }

        boolean canInitiateBalancing = System.currentTimeMillis() > this.lastAttemptStartBalanceTime + balanceAttemptIntervalMillis;

        if (!canInitiateBalancing) {
            return;
        }

        Transaction startBalanceTransactionSignal;

        if (!isMultiLayerBalancer && this.messageSingleLayerSentCounter == MAX_ATTEMPTS_SEND_START_BALANCE) {
            this.messageSingleLayerSentCounter = 0;
        }

        if (this.messageSingleLayerSentCounter < MAX_ATTEMPTS_SEND_START_BALANCE) {
            startBalanceTransactionSignal = new Status(sourceIdentifier, targetGroup, true, currentDeviceCount, transaction.getAvgLoad(), false);
            this.sendTransaction(startBalanceTransactionSignal);
            this.lastAttemptStartBalanceTime = System.currentTimeMillis();
            this.messageSingleLayerSentCounter++;
            return;
        }

        if (this.messageMultiLayerSentCounter == MAX_ATTEMPTS_SEND_START_BALANCE) {
            this.messageMultiLayerSentCounter = 0;
        }

        if (this.messageMultiLayerSentCounter < MAX_ATTEMPTS_SEND_START_BALANCE) {
            startBalanceTransactionSignal = new LBMultiRequest(sourceIdentifier, targetGroup);
            this.sendTransaction(startBalanceTransactionSignal);
            this.lastAttemptStartBalanceTime = System.currentTimeMillis();
            this.messageMultiLayerSentCounter++;
        }
    }

    protected Long qtyMaxTimeResendTransaction() {
        return this.configs.getMaxTryResendTransaction();
    }

    protected Long getLBStartReplyTimeWaiting() {
        return this.configs.getLBStartReplyTimeout();
    }

    protected Long getLBDeviceRecivedReplyTimeWaiting() {
        return this.configs.getLBStartReplyTimeout() * 2;
    }

    protected Long getLBRequestTimeWaiting() {
        return this.configs.getLBStartReplyTimeout() * 2;
    }

    protected String getGatewayGroup() {
        return this.groupManager.getGroup();
    }

    protected boolean canReciveNewDevice() {
        return this.internalStatus.getAvailable();
    }

    protected boolean isMultiLayerBalancer() {
        return this.configs.isMultiLayerBalancing();
    }

    protected boolean isValidPublishMessageInterval(long publishedAt) {
        return this.configs.validPublishMessageInterval(publishedAt);
    }

    public void cancelTimeout() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(true);
        }
    }

    public void transitionTo(BalancerState newState) {
        String oldStateName = (this.state != null) ? this.state.getClass().getSimpleName() : "null";
        String newStateName = newState.getClass().getSimpleName();

        logger.log(Level.INFO, "Transição de estado: {0} -> {1}", new Object[]{oldStateName, newStateName});

        this.state = newState;
        this.state.onEnter();
    }

    public void scheduleTimeout(long delayMillis) {
        this.cancelTimeout();
        timeoutFuture = scheduler.schedule(state::onTimeout, delayMillis, TimeUnit.MILLISECONDS);
    }

    protected Optional<Device> getFirstDevice() throws IOException {
        List<Device> devices = this.deviceManager.getAllDevices();
        if (devices.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(devices.get(0));
    }

    protected String buildSource() {
        return new StringBuilder(this.groupManager.getGroup())
                .append("/")
                .append(this.idManager.getIP())
                .append("/")
                .append(this.configs.getMqttPort())
                .toString();
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
        this.state.handle((Transaction) trans);
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

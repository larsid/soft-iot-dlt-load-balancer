package dlt.load.balancer.model;

import static java.util.stream.Collectors.toList;

import br.uefs.larsid.extended.mapping.devices.services.IDevicePropertiesManager;
import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import com.google.gson.JsonObject;
import dlt.auth.services.IPublisher;
import dlt.client.tangle.hornet.enums.TransactionType;
import static dlt.client.tangle.hornet.enums.TransactionType.LB_ENTRY;
import static dlt.client.tangle.hornet.enums.TransactionType.LB_ENTRY_REPLY;
import static dlt.client.tangle.hornet.enums.TransactionType.LB_REQUEST;
import dlt.client.tangle.hornet.model.transactions.LBDevice;
import dlt.client.tangle.hornet.model.transactions.LBMultiDevice;
import dlt.client.tangle.hornet.model.transactions.LBMultiDeviceResponse;
import dlt.client.tangle.hornet.model.transactions.LBMultiRequest;
import dlt.client.tangle.hornet.model.transactions.LBMultiResponse;
import dlt.client.tangle.hornet.model.transactions.LBReply;
import dlt.client.tangle.hornet.model.transactions.Reply;
import dlt.client.tangle.hornet.model.transactions.Request;
import dlt.client.tangle.hornet.model.transactions.Status;
import dlt.client.tangle.hornet.model.transactions.TargetedTransaction;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import dlt.client.tangle.hornet.services.ILedgerSubscriber;
import dlt.id.manager.services.IDLTGroupManager;
import dlt.id.manager.services.IIDManagerService;
import java.io.IOException;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.json.JSONArray;

/**
 *
 * @author  Allan Capistrano, Antonio Crispim, Uellington Damasceno
 * @version 0.0.3
 */
public class Balancer implements ILedgerSubscriber, Runnable {

  private final ScheduledExecutorService executor;
  private LedgerConnector connector;
  private final Long TIMEOUT_LB_REPLY;
  private final Long TIMEOUT_GATEWAY;
  private Transaction lastTransaction;
  private List<String> subscribedTopics;
  private IDevicePropertiesManager deviceManager;
  private IIDManagerService idManager;
  private IDLTGroupManager groupManager;
  private IPublisher iPublisher;
  private TimerTask timerTaskLb;
  private TimerTask timerTaskGateWay;
  private int timerPass;
  private int resent;
  private Status lastStatus;
  private String lastRemovedDevice;
  private boolean flagSubscribe;
  private static final Logger logger = Logger.getLogger(Balancer.class.getName());
  private boolean balanceable, multiLayerBalancing;
  private String realMqttPort;

  ExecutorService executorTimeout = Executors.newSingleThreadExecutor();

  public Balancer(long timeoutLB, long timeoutGateway) {
    this.TIMEOUT_LB_REPLY = timeoutLB;
    this.TIMEOUT_GATEWAY = timeoutGateway;

    this.buildTimerResendTransaction();
    this.resent = 0;
    this.lastStatus = null;
    this.flagSubscribe = true;

    this.executor = Executors.newSingleThreadScheduledExecutor();
  }

  public void buildTimerTaskLB() {
    timerTaskLb =
      new TimerTask() {
        @Override
        public void run() {
          Logger log = Logger.getLogger(Balancer.class.getName());

          timerPass = timerPass + 1;
          log.info(String.valueOf(timerPass));

          if ((timerPass * 1000) >= TIMEOUT_LB_REPLY) {
            lastTransaction = null;
            timerTaskLb.cancel();
            log.info("TIME'S UP buildTimerTaskLB");
          }
        }
      };
  }

  public void buildTimerResendTransaction() {
    timerTaskGateWay = new TimerTask() {
        @Override
        public void run() {
          Logger log = Logger.getLogger(Balancer.class.getName());

          timerPass = timerPass + 1;
          log.info(String.valueOf(timerPass));

          if ((timerPass * 1000) >= TIMEOUT_GATEWAY) {
            resendTransaction();
            timerTaskGateWay.cancel();
            log.info("TIME'S UP buildTimerResendTransaction");
          }
        }
    };
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

  public void setSubscribedTopics(String topicsJSON) {
    logger.log(Level.INFO, "Set topics: {0}", topicsJSON);

    this.subscribedTopics =
      new JSONArray(topicsJSON)
        .toList()
        .stream()
        .filter(String.class::isInstance)
        .map(String.class::cast)
        .peek(s -> {
            logger.log(Level.INFO, "Load balancer - Subscribe at {0}", s);
        }).collect(toList());
  }

  public void start() {
    this.executor.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS);
    this.balanceable = this.isBalanceable();
    logger.log(Level.INFO, "IS BALANCEABLE: {0}", this.balanceable);
    this.multiLayerBalancing = this.isMultiLayerBalancing();
    logger.log(Level.INFO, "IS MULTI LAYER BALANCING: {0}", this.multiLayerBalancing);
    this.realMqttPort = this.currentMqttPort();
    logger.log(Level.INFO, "Real MQTT PORT: {0}", this.realMqttPort);
  }

  public void stop() {
    this.flagSubscribe = true;

    this.subscribedTopics.forEach(topic ->
        this.connector.unsubscribe(topic, this)
    );

    this.executor.shutdown();

    try {
      if (
        this.executor.awaitTermination(500, TimeUnit.MILLISECONDS) &&
        !this.executor.isShutdown()
      ) {
        this.executor.shutdownNow();
      }
    } catch (InterruptedException ex) {
      this.executor.shutdownNow();
    }
  }

  private void messageArrived(Transaction transaction) {
    logger.info("Waiting for LB_ENTRY OR LB_MULTI_REQUEST");
    logger.info("Load - Last transaction is Null");
    String source = this.buildSource();

    if (
      transaction.is(TransactionType.LB_STATUS) &&
      transaction.isLoopback(source)
    ) {
        this.lastStatus = (Status) transaction;
        return;
    }
    
    if (!transaction.is(TransactionType.LB_ENTRY) 
            || !transaction.is(TransactionType.LB_MULTI_DEVICE_REQUEST)) {
        return;
    }
    
    if (transaction.isLoopback(source)) {
        // Definindo minha última transação enviada.
        this.lastTransaction = transaction;
        executeTimeOutLB();
        return;
    } 
    TransactionType type = transaction.getType();
    logger.log(Level.INFO, "Receive message type: {0}", type);

    if (!this.lastStatus.getAvailable()) {
        logger.info("Is not avaliable to receive new device.");
        return;
    }
    
    String group = this.groupManager.getGroup();
    String newTarget = transaction.getSource();

    Transaction transactionReply = type.isMultiLayer() 
            ? new LBMultiResponse(source, group, newTarget)
            : new LBReply(source, group, newTarget);
    
    this.sendTransaction(transactionReply);

  }

  private void processTransactions(Transaction transaction) {
    String source = this.buildSource();
    
    logger.info("Processing transaction...");
    
    // Somente se as transações recebidas não forem enviadas pelo próprio gateway.
    if (transaction.isLoopback(source)) {
        return;
    }
    String group = this.groupManager.getGroup();
    String newTarget = transaction.getSource();
    
    switch (lastTransaction.getType()) {
      case LB_ENTRY:
        if(!transaction.is(TransactionType.LB_ENTRY_REPLY)){
            return;
        }
        
        if(((TargetedTransaction) transaction).isSameTarget(source)) {
          timerTaskLb.cancel();
          timerTaskGateWay.cancel();
          
          // Enviar LB_REQUEST.
          try {
            Device deviceToSend = this.deviceManager.getAllDevices().get(0);
            String deviceStringToSend = DeviceWrapper.toJSON(deviceToSend);
            this.lastRemovedDevice = deviceStringToSend;

            Transaction transactionRequest = new Request(
              source,
              group,
              deviceStringToSend,
              newTarget
            );
            this.sendTransaction(transactionRequest);

            timerTaskGateWay.cancel();
            executeTimeOutGateWay();
          } catch (IOException ioe) {
            logger.info("Load Balancer - Error! Unable to retrieve device list.");
            ioe.printStackTrace();
          }
        } else {
          timerTaskGateWay.cancel();
          executeTimeOutGateWay();
        }

        break;
      case LB_ENTRY_REPLY:
        if(!transaction.is(TransactionType.LB_REQUEST)){
            return;
        }
        if (((TargetedTransaction) transaction).isSameTarget(source)) {
          timerTaskGateWay.cancel();
          // Carregar dispositivo na lista.
          String device = ((Request) transaction).getDevice();
          this.loadSwapReceberDispositivo(device);

          // Enviar LB_REPLY.
          Transaction transactionReply = new Reply(source, group, newTarget);
          this.sendTransaction(transactionReply);

          // Colocar para a última transação ser nula.
          this.lastTransaction = null;
        } else {
          timerTaskGateWay.cancel();
          executeTimeOutGateWay();
        }

        break;
      case LB_REQUEST:
        if(!transaction.is(TransactionType.LB_REPLY)){
            return;
        }
        if (((TargetedTransaction) transaction).isSameTarget(source)) {
          timerTaskGateWay.cancel();
          
          try {
            String ip = transaction.getSource().split("/")[2]; //Só funcionará se o group tiver um '/' EX: cloud/c1 - Alterar
            String port = transaction.getSource().split("/")[3]; //Idem para acima
            this.removeFirstDevice(ip, port);
            
            Transaction transactionDevice = new LBDevice(
              source,
              group,
              this.lastRemovedDevice,
              newTarget
            );

            this.sendTransaction(transactionDevice);

            // Colocar para a última transação ser nula.
            this.lastTransaction = null;
          } catch (MqttException me) {
            logger.info("Load Balancer - Error! Unable to remove the first device.");
            me.printStackTrace();
          }
        } else {
          timerTaskGateWay.cancel();
          executeTimeOutGateWay();
        }

        break;
      case LB_MULTI_REQUEST:
          if(!transaction.is(TransactionType.LB_MULTI_RESPONSE)){
              return;
          }
          
          if(((TargetedTransaction) transaction).isSameTarget(source)) {
            timerTaskLb.cancel();
            timerTaskGateWay.cancel();

            try {
                Device deviceToSend = this.deviceManager.getAllDevices().get(0);
                String deviceStringToSend = DeviceWrapper.toJSON(deviceToSend);
                this.lastRemovedDevice = deviceStringToSend;

                Transaction transactionRequest = new LBMultiDevice(
                  source,
                  group,
                  deviceStringToSend,
                  newTarget
                );
                this.sendTransaction(transactionRequest);

                timerTaskGateWay.cancel();
                executeTimeOutGateWay();
            } catch (IOException ioe) {
              logger.info("Load Balancer - Error! Unable to retrieve device list.");
              ioe.printStackTrace();
            }
        } 
          else {
          timerTaskGateWay.cancel();
          executeTimeOutGateWay();
        }
        break;
      case LB_MULTI_RESPONSE:
        if(!transaction.is(TransactionType.LB_MULTI_DEVICE_REQUEST)){
            return;
        }
        if (((TargetedTransaction) transaction).isSameTarget(source)) {
          timerTaskGateWay.cancel();

          String device = ((LBMultiDevice) transaction).getDevice();
          this.loadSwapReceberDispositivo(device);

          Transaction response = new LBMultiDeviceResponse(source, group, device, newTarget);
          this.sendTransaction(response);

          this.lastTransaction = null;
        } else {
          timerTaskGateWay.cancel();
          executeTimeOutGateWay();
        }
        break;
      case LB_MULTI_DEVICE_REQUEST:
          if(!transaction.is(TransactionType.LB_MULTI_DEVICE_RESPONSE)){
              return;
          }
          if (((TargetedTransaction) transaction).isSameTarget(source)) {
            timerTaskGateWay.cancel();

            try {
              String ip = transaction.getSource().split("/")[2];
              String port = transaction.getSource().split("/")[3];
              this.removeFirstDevice(ip, port);
              this.lastTransaction = null;
            } catch (MqttException me) {
              logger.info("Load Balancer - Error! Unable to remove the first device.");
              me.printStackTrace();
            }
        } else {
          timerTaskGateWay.cancel();
          executeTimeOutGateWay();
        }
        break;
      case LB_MULTI_DEVICE_RESPONSE:
        logger.info(TransactionType.LB_MULTI_DEVICE_RESPONSE.name());
        break;
      default:
        logger.info("Error! Something went wrong.");
        break;
    }
  }

  private void executeTimeOutGateWay() {
    timerTaskGateWay = new TimerTask() {
        @Override
        public void run() {
            Logger log = Logger.getLogger(Balancer.class.getName());

            timerPass = timerPass + 1;
            log.info(String.valueOf(timerPass));

            if ((timerPass * 1000) >= TIMEOUT_LB_REPLY) {
              lastTransaction = null;
              timerTaskGateWay.cancel();
              log.info("TIME'S UP executeTimeOutGateWay");
            }
        }
    };

    timerPass = 0;
    Timer timer = new Timer();
    timer.scheduleAtFixedRate(timerTaskGateWay, 1000, 1000);
  }

  private void executeTimeOutLB() {
    timerTaskLb = new TimerTask() {
        @Override
        public void run() {
          Logger log = Logger.getLogger(Balancer.class.getName());

          timerPass = timerPass + 1;
          log.info(String.valueOf(timerPass));

          if ((timerPass * 1000) >= TIMEOUT_LB_REPLY) {
            log.info("TIME'S UP executeTimeOutLB");
            if(multiLayerBalancing){
                log.info("Send multi-layer balancing request.");
                Transaction trans = new LBMultiRequest(buildSource(), groupManager.getGroup());
                sendTransaction(trans);
            }else{
                lastTransaction = null;
            }
            timerTaskLb.cancel();
          }
        }
    };

    timerPass = 0;
    Timer timer = new Timer();
    timer.scheduleAtFixedRate(timerTaskLb, 1000, 1000);
  }

  private void sendTransaction(Transaction transaction) {
    try {
      this.connector.put(transaction);
      this.lastTransaction = transaction;
    } catch (InterruptedException ie) {
      logger.info("Load Balancer - Error commit transaction.");
      ie.printStackTrace();
    }
  }

  private void resendTransaction() {
    try {
      this.resent += 1;
      if (this.resent == 3) {
        this.lastTransaction = null;
        this.resent = 0;
        return;
      }

      this.connector.put(lastTransaction);
      Timer timer = new Timer();
      timerPass = 0;
      timer.scheduleAtFixedRate(timerTaskLb, 1000, 1000);
    } catch (InterruptedException ie) {
      logger.info("Load Balancer - Error commit transaction.");
      ie.printStackTrace();
    }
  }

  private String buildSource() {
    return new StringBuilder(this.groupManager.getGroup())
      .append("/")
      .append(this.idManager.getIP())
      .append("/")
      .append(this.currentMqttPort())
      .toString();
  }

  public void removeFirstDevice(String targetIp, String targetMqttPort) throws MqttException {
    try {
      List<Device> allDevices = deviceManager.getAllDevices();

      if (!allDevices.isEmpty()) {
        Device deviceARemover = allDevices.get(0);
        JsonObject jsonPublish = new JsonObject();
        jsonPublish.addProperty("id", deviceARemover.getId());
        jsonPublish.addProperty("url", "tcp://" + targetIp);
        jsonPublish.addProperty("port", targetMqttPort);
        jsonPublish.addProperty("user", "karaf");
        jsonPublish.addProperty("password", "karaf");

        iPublisher.publish(
          deviceARemover.getId(),
          "SET VALUE brokerMqtt{" + jsonPublish.toString() + "}"
        );

        deviceManager.removeDevice(allDevices.get(0).getId());
      }
    } catch (IOException ioe) {
      logger.info(
          "Error! To retrieve device list or to remove the first device."
        );
      ioe.printStackTrace();
    }
  }

  public void loadSwapReceberDispositivo(String deviceJSON) {
    try {
      logger.log(Level.INFO, "DeviceJSON: {0}", deviceJSON);

      Device device = DeviceWrapper.toDevice(deviceJSON);
      logger.log(Level.INFO, "Device after convert: {0}", DeviceWrapper.toJSON(device));

      deviceManager.addDevice(device);
    } catch (IOException ioe) {
      logger.info("Error! To add a new device to the list.");
      ioe.printStackTrace();
    }
  }

  public void stopTimeout() {
    while (!executorTimeout.isShutdown());
  }

  @Override
  public void update(Object trans, Object messageId) {
      
    if(!this.balanceable){
        logger.log(Level.INFO, "Load balancer - New message but will not processed because is not balanceable.");
        return;
    }
    
    if (!(trans instanceof Transaction)) {
        logger.log(Level.INFO, "Load balancer - New message with id: {0} is Invalid", messageId); 
        return;
    }
    
    Transaction transaction = (Transaction) trans;
    logger.log(Level.INFO, "Load balancer - New message with id {0} is Valid", messageId);

    logger.log(Level.INFO, "Message id: {0}", messageId);
    
    if(transaction.isMultiLayerTransaction() && !this.multiLayerBalancing){
        logger.info("Load balancer - Multilayer message type not allowed.");
        return;
    }
    // Evitar o processamento de mensagens antigas.
    if (System.currentTimeMillis() - transaction.getPublishedAt() < 15000 ) {
      if (lastTransaction != null) {
        this.processTransactions(transaction);
      } else {
        this.messageArrived(transaction);
      }
    }
  }

  /* Thread para verificar o IP do client tangle e se inscrever nos tópicos
    de interesse.*/
  @Override
  public void run() {
    try {
        URL urlObj = new URL(this.connector.getLedgerWriter().getUrl());
        String host = urlObj.getHost(); 
        InetAddress inet = InetAddress.getByName(host);
    
      if (inet.isReachable(5000)) {
        if (this.flagSubscribe) {
          this.subscribedTopics.forEach(topic ->
              this.connector.subscribe(topic, this)
            );

          this.flagSubscribe = false;
        }
      } else {
        this.flagSubscribe = true;
      }
    } catch (UnknownHostException uhe) {
      logger.info("Error! Unknown Host.");
      uhe.printStackTrace();
    } catch (IOException ioe) {
      logger.info("Error! Can't connect to InetAddress.");
      ioe.printStackTrace();
    }
  }

   private boolean isBalanceable() {
        String isBalanceableValue = System.getenv("IS_BALANCEABLE");
        if(isBalanceableValue == null){
           logger.info("Load balancer - IS_BALANCEABLE env var is not defined.");
        }
        return isBalanceableValue != null && isBalanceableValue.equals("1");
    }

    private boolean isMultiLayerBalancing() {
        String isMultiLayer = System.getenv("IS_MULTI_LAYER");
        if(isMultiLayer == null){
            logger.info("Load balancer - IS_MULTI_LAYER env var is not defined.");
        }
        return isMultiLayer != null && isMultiLayer.equals("1");
    }
    
    private String currentMqttPort(){
        String port = System.getenv("GATEWAY_PORT");
        return port == null ? "1883" : port;
    }
    
}

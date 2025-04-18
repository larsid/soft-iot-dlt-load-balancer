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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
import org.json.JSONException;

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
    topicsJSON = Optional
            .ofNullable(System.getenv("TOPICS"))
            .orElse(topicsJSON);
    try {
        JSONArray array = new JSONArray(topicsJSON);
        this.subscribedTopics = array.toList().stream()
            .filter(String.class::isInstance)
            .map(String.class::cast)
            .peek(topic -> logger.log(Level.INFO, "Topic read: {0}", topic))
            .collect(toList());
    } catch (JSONException e) {
        logger.log(Level.SEVERE, "Formato inválido para TOPICS: " + topicsJSON, e);
        this.subscribedTopics = List.of("LB_*");
    }
  }

  public void start() {
    this.setSubscribedTopics("");
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
      logger.info("Esperando LB_ENTRY ou LB_MULTI_REQUEST...");

      String source = this.buildSource();
      logger.log(Level.INFO, "Source gerado: {0}", source);

      if (transaction.is(TransactionType.LB_STATUS) && transaction.isLoopback(source)) {
          logger.info("Transação LB_STATUS de loopback recebida. Atualizando lastStatus.");
          this.lastStatus = (Status) transaction;
          return;
      }

      if (!transaction.is(TransactionType.LB_ENTRY) && 
          !transaction.is(TransactionType.LB_MULTI_DEVICE_REQUEST)) {
          logger.log(Level.INFO, "Transação ignorada. Tipo não é LB_ENTRY nem LB_MULTI_DEVICE_REQUEST.");
          return;
      }

      if (transaction.isLoopback(source)) {
          logger.info("Transação de loopback detectada. Salvando como lastTransaction e executando timeout.");
          this.lastTransaction = transaction;
          executeTimeOutLB();
          return;
      }

      TransactionType type = transaction.getType();
      
      if (!this.lastStatus.getAvailable()) {
          logger.info("Gateway indisponível para receber novos devices.");
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
      logger.info("Iniciando processamento da transação...");

      if (transaction.isLoopback(source)) {
          logger.info("Transação de loopback ignorada.");
          return;
      }

      String group = this.groupManager.getGroup();
      String newTarget = transaction.getSource();
      TransactionType lastType = lastTransaction.getType();
      logger.log(Level.INFO, "Última transação registrada: {0}", lastType);
      
      switch (lastType) {
          case LB_ENTRY:
              if (!transaction.is(TransactionType.LB_ENTRY_REPLY)) {
                  logger.info("Transação ignorada. Esperado LB_ENTRY_REPLY.");
                  return;
              }

              if (((TargetedTransaction) transaction).isSameTarget(source)) {
                  logger.info("LB_ENTRY_REPLY recebido com target correto. Iniciando envio de LB_REQUEST.");

                  timerTaskLb.cancel();
                  timerTaskGateWay.cancel();

                  try {
                      Device deviceToSend = this.deviceManager.getAllDevices().get(0);
                      String deviceStringToSend = DeviceWrapper.toJSON(deviceToSend);
                      this.lastRemovedDevice = deviceStringToSend;

                      logger.log(Level.INFO, "Device selecionado para envio: {0}", deviceStringToSend);

                      Transaction transactionRequest = new Request(source, group, deviceStringToSend, newTarget);
                      this.sendTransaction(transactionRequest);

                      logger.info("LB_REQUEST enviado com sucesso.");
                      timerTaskGateWay.cancel();
                      executeTimeOutGateWay();
                  } catch (IOException ioe) {
                      logger.info("Erro ao obter lista de devices.");
                      ioe.printStackTrace();
                  }
              } else {
                  logger.info("LB_ENTRY_REPLY com target diferente. Timeout reiniciado.");
                  timerTaskGateWay.cancel();
                  executeTimeOutGateWay();
              }
              break;

          case LB_ENTRY_REPLY:
              if (!transaction.is(TransactionType.LB_REQUEST)) {
                  logger.info("Transação ignorada. Esperado LB_REQUEST.");
                  return;
              }

              if (((TargetedTransaction) transaction).isSameTarget(source)) {
                  logger.info("LB_REQUEST recebido corretamente. Adicionando device local.");

                  timerTaskGateWay.cancel();

                  String device = ((Request) transaction).getDevice();
                  this.loadSwapReceberDispositivo(device);

                  Transaction transactionReply = new Reply(source, group, newTarget);
                  this.sendTransaction(transactionReply);

                  logger.info("LB_REPLY enviado. Resetando lastTransaction.");
                  this.lastTransaction = null;
              } else {
                  logger.info("LB_REQUEST com target errado. Timeout reiniciado.");
                  timerTaskGateWay.cancel();
                  executeTimeOutGateWay();
              }
              break;

          case LB_REQUEST:
              if (!transaction.is(TransactionType.LB_REPLY)) {
                  logger.info("Transação ignorada. Esperado LB_REPLY.");
                  return;
              }

              if (((TargetedTransaction) transaction).isSameTarget(source)) {
                  logger.info("LB_REPLY recebido. Removendo device do gateway local.");

                  timerTaskGateWay.cancel();

                  try {
                      String ip = transaction.getSource().split("/")[2];
                      String port = transaction.getSource().split("/")[3];

                      logger.log(Level.INFO, "IP destino: {0}, Porta destino: {1}", new Object[]{ip, port});
                      this.removeFirstDevice(ip, port);

                      Transaction transactionDevice = new LBDevice(source, group, this.lastRemovedDevice, newTarget);
                      this.sendTransaction(transactionDevice);

                      logger.info("LB_DEVICE enviado. Resetando lastTransaction.");
                      this.lastTransaction = null;
                  } catch (MqttException me) {
                      logger.info("Erro ao remover device.");
                      me.printStackTrace();
                  }
              } else {
                  logger.info("LB_REPLY com target errado. Timeout reiniciado.");
                  timerTaskGateWay.cancel();
                  executeTimeOutGateWay();
              }
              break;

          case LB_MULTI_REQUEST:
              if (!transaction.is(TransactionType.LB_MULTI_RESPONSE)) {
                  logger.info("Transação ignorada. Esperado LB_MULTI_RESPONSE.");
                  return;
              }

              if (((TargetedTransaction) transaction).isSameTarget(source)) {
                  logger.info("LB_MULTI_RESPONSE recebido corretamente. Preparando envio de LB_MULTI_DEVICE.");

                  timerTaskLb.cancel();
                  timerTaskGateWay.cancel();

                  try {
                      Device deviceToSend = this.deviceManager.getAllDevices().get(0);
                      String deviceStringToSend = DeviceWrapper.toJSON(deviceToSend);
                      this.lastRemovedDevice = deviceStringToSend;

                      logger.log(Level.INFO, "Device selecionado: {0}", deviceStringToSend);

                      Transaction transactionRequest = new LBMultiDevice(source, group, deviceStringToSend, newTarget);
                      this.sendTransaction(transactionRequest);

                      logger.info("LB_MULTI_DEVICE enviado com sucesso.");
                      timerTaskGateWay.cancel();
                      executeTimeOutGateWay();
                  } catch (IOException ioe) {
                      logger.info("Erro ao acessar device list.");
                      ioe.printStackTrace();
                  }
              } else {
                  logger.info("LB_MULTI_RESPONSE com target errado. Timeout reiniciado.");
                  timerTaskGateWay.cancel();
                  executeTimeOutGateWay();
              }
              break;

          case LB_MULTI_RESPONSE:
              if (!transaction.is(TransactionType.LB_MULTI_DEVICE_REQUEST)) {
                  logger.info("Transação ignorada. Esperado LB_MULTI_DEVICE_REQUEST.");
                  return;
              }

              if (((TargetedTransaction) transaction).isSameTarget(source)) {
                  logger.info("LB_MULTI_DEVICE_REQUEST recebido corretamente. Adicionando device local.");

                  timerTaskGateWay.cancel();

                  String device = ((LBMultiDevice) transaction).getDevice();
                  this.loadSwapReceberDispositivo(device);

                  Transaction response = new LBMultiDeviceResponse(source, group, device, newTarget);
                  this.sendTransaction(response);

                  logger.info("LB_MULTI_DEVICE_RESPONSE enviado. Resetando lastTransaction.");
                  this.lastTransaction = null;
              } else {
                  logger.info("LB_MULTI_DEVICE_REQUEST com target errado. Timeout reiniciado.");
                  timerTaskGateWay.cancel();
                  executeTimeOutGateWay();
              }
              break;

          case LB_MULTI_DEVICE_REQUEST:
              if (!transaction.is(TransactionType.LB_MULTI_DEVICE_RESPONSE)) {
                  logger.info("Transação ignorada. Esperado LB_MULTI_DEVICE_RESPONSE.");
                  return;
              }

              if (((TargetedTransaction) transaction).isSameTarget(source)) {
                  logger.info("LB_MULTI_DEVICE_RESPONSE recebido corretamente. Removendo device do gateway local.");

                  timerTaskGateWay.cancel();

                  try {
                      String ip = transaction.getSource().split("/")[2];
                      String port = transaction.getSource().split("/")[3];

                      logger.log(Level.INFO, "IP destino: {0}, Porta destino: {1}", new Object[]{ip, port});
                      this.removeFirstDevice(ip, port);

                      this.lastTransaction = null;
                  } catch (MqttException me) {
                      logger.info("Erro ao remover device.");
                      me.printStackTrace();
                  }
              } else {
                  logger.info("LB_MULTI_DEVICE_RESPONSE com target errado. Timeout reiniciado.");
                  timerTaskGateWay.cancel();
                  executeTimeOutGateWay();
              }
              break;

          case LB_MULTI_DEVICE_RESPONSE:
              logger.info("Transação LB_MULTI_DEVICE_RESPONSE recebida. Nada a fazer.");
              break;

          default:
              logger.info("Tipo de transação não reconhecido ou não tratado.");
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
    
    if(transaction.isMultiLayerTransaction() && !this.multiLayerBalancing){
        logger.info("Load balancer - Multilayer message type not allowed.");
        return;
    }
    
    logger.log(Level.INFO, "Transação recebida: {0}", transaction);

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
           String ledgerUrl = this.connector.getLedgerWriter().getUrl();
           logger.log(Level.INFO, "Verificando conectividade com: {0}", ledgerUrl);

           URL urlObj = new URL(ledgerUrl);
           String host = urlObj.getHost(); 
           InetAddress inet = InetAddress.getByName(host);

           if (inet.isReachable(5000)) {
               logger.log(Level.INFO, "Conectado com sucesso a: {0}", host);

               if (this.flagSubscribe) {
                   String topics = Arrays.toString(this.subscribedTopics.toArray());
                   logger.log(Level.INFO, "Iniciando processo de inscrição nos tópicos: {0}", topics);
                  
                   this.subscribedTopics.forEach(topic -> {
                       logger.log(Level.INFO, "Inscrevendo-se no tópico: {0}", topic);
                       this.connector.subscribe(topic, this);
                   });

                   this.flagSubscribe = false;
                   logger.info("Inscrição concluída. flagSubscribe setado para false.");
               } else {
                   logger.fine("Já inscrito. Nenhuma ação necessária.");
               }
           } else {
               logger.log(Level.WARNING, "Host inacessível: {0}. flagSubscribe setado para true.", host);
               this.flagSubscribe = true;
           }

       } catch (UnknownHostException uhe) {
           logger.log(Level.SEVERE, "Erro: Host desconhecido.", uhe);
       } catch (IOException ioe) {
           logger.log(Level.SEVERE, "Erro: Falha ao conectar com o endereço.", ioe);
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

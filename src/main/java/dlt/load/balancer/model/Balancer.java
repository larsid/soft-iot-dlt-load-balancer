package dlt.load.balancer.model;

import static java.util.stream.Collectors.toList;

import br.uefs.larsid.extended.mapping.devices.services.IDevicePropertiesManager;
import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import com.google.gson.JsonObject;
import dlt.auth.services.IPublisher;
import dlt.client.tangle.enums.TransactionType;
import dlt.client.tangle.model.transactions.LBDevice;
import dlt.client.tangle.model.transactions.LBReply;
import dlt.client.tangle.model.transactions.Reply;
import dlt.client.tangle.model.transactions.Request;
import dlt.client.tangle.model.transactions.Status;
import dlt.client.tangle.model.transactions.TargetedTransaction;
import dlt.client.tangle.model.transactions.Transaction;
import dlt.client.tangle.services.ILedgerSubscriber;
import dlt.id.manager.services.IDLTGroupManager;
import dlt.id.manager.services.IIDManagerService;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  private Logger log;

  final Duration timeout = Duration.ofSeconds(40);
  ExecutorService executorTimeout = Executors.newSingleThreadExecutor();

  public Balancer(long timeoutLB, long timeoutGateway) {
    this.TIMEOUT_LB_REPLY = timeoutLB;
    this.TIMEOUT_GATEWAY = timeoutGateway;

    this.buildTimerResendTransaction();
    this.resent = 0;
    this.lastStatus = null;
    this.flagSubscribe = true;

    this.executor = Executors.newSingleThreadScheduledExecutor();

    this.log = Logger.getLogger(Balancer.class.getName());
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
            log.info("TIME'S UP");
          }
        }
      };
  }

  public void buildTimerResendTransaction() {
    timerTaskGateWay =
      new TimerTask() {
        @Override
        public void run() {
          Logger log = Logger.getLogger(Balancer.class.getName());

          timerPass = timerPass + 1;
          log.info(String.valueOf(timerPass));

          if ((timerPass * 1000) >= TIMEOUT_GATEWAY) {
            resendTransaction();
            timerTaskGateWay.cancel();
            log.info("TIME'S UP");
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
    this.log.info("Set topics: " + topicsJSON);

    this.subscribedTopics =
      new JSONArray(topicsJSON)
        .toList()
        .stream()
        .filter(String.class::isInstance)
        .map(String.class::cast)
        .collect(toList());
  }

  public void start() {
    this.executor.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS);
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
    this.log.info("Waiting for LB_ENTRY");
    this.log.info("Load  - Last transaction is Null");

    if (
      transaction.getType().equals(TransactionType.LB_STATUS) &&
      transaction.getSource().equals(this.buildSource())
    ) {
      this.lastStatus = (Status) transaction;
    }

    if (transaction.getType().equals(TransactionType.LB_ENTRY)) {
      if (transaction.getSource().equals(this.buildSource())) {
        // Definindo minha última transação enviada.
        this.lastTransaction = transaction;

        executeTimeOutLB();
      } else {
        // Caso seja de outro gateway.
        this.log.info("Receive: " + TransactionType.LB_ENTRY);

        // Verificando a própria disponibilidade.
        if (this.lastStatus.getAvailable()) {
          String source = this.buildSource();
          String group = this.groupManager.getGroup();
          String newTarget = transaction.getSource();

          Transaction transactionReply = new LBReply(source, group, newTarget);
          this.sendTransaction(transactionReply);
        }
      }
    }
  }

  private void processTransactions(Transaction transaction) {
    this.log.info("processing transaction...");

    // Somente se as transações recebidas não forem enviadas pelo próprio gateway.
    if (!transaction.getSource().equals(this.buildSource())) {
      switch (lastTransaction.getType()) {
        case LB_ENTRY:
          if (
            transaction.getType().equals(TransactionType.LB_ENTRY_REPLY) &&
            ((TargetedTransaction) transaction).getTarget()
              .equals(this.buildSource())
          ) {
            timerTaskLb.cancel();
            timerTaskGateWay.cancel();

            String source = this.buildSource();
            String group = this.groupManager.getGroup();
            String newTarget = transaction.getSource();

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

              executeTimeOutGateWay();
            } catch (IOException ioe) {
              this.log.info(
                  "Load Balancer - Error! Unable to retrieve device list."
                );
              ioe.printStackTrace();
            }
          } else if (transaction.getType().equals(TransactionType.LB_ENTRY_REPLY) &&
            !((TargetedTransaction) transaction).getTarget()
              .equals(this.buildSource())) {
            executeTimeOutGateWay();
          }

          break;
        case LB_ENTRY_REPLY:
          if (
            transaction.getType().equals(TransactionType.LB_REQUEST) &&
            ((TargetedTransaction) transaction).getTarget()
              .equals((this.buildSource()))
          ) {
            timerTaskGateWay.cancel();

            String source = this.buildSource();
            String group = this.groupManager.getGroup();
            String newTarget = transaction.getSource();

            // Carregar dispositivo na lista.
            String device = ((Request) transaction).getDevice();
            this.loadSwapReceberDispositivo(device);

            // Enviar LB_REPLY.
            Transaction transactionReply = new Reply(source, group, newTarget);
            this.sendTransaction(transactionReply);

            // Colocar para a última transação ser nula.
            this.lastTransaction = null;
          } else if (transaction.getType().equals(TransactionType.LB_REQUEST) &&
            !((TargetedTransaction) transaction).getTarget()
              .equals(this.buildSource())) {
            executeTimeOutGateWay();
          }

          break;
        case LB_REQUEST:
          if (
            transaction.getType().equals(TransactionType.LB_REPLY) &&
            ((TargetedTransaction) transaction).getTarget()
              .equals((this.buildSource()))
          ) {
            timerTaskGateWay.cancel();

            String source = this.buildSource();
            String group = this.groupManager.getGroup();
            String newTarget = transaction.getSource();

            try {
              this.removeFirstDevice(transaction.getSource().split("/")[2]);

              // Enviar LBDevice.
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
              this.log.info(
                  "Load Balancer - Error! Unable to remove the first device."
                );
              me.printStackTrace();
            }
          } else if (transaction.getType().equals(TransactionType.LB_REPLY) &&
            !((TargetedTransaction) transaction).getTarget()
              .equals(this.buildSource())) {
            executeTimeOutGateWay();
          }

          break;
        default:
          this.log.info("Error! Something went wrong.");

          break;
      }
    }
  }

  private void executeTimeOutGateWay() {
    timerTaskGateWay =
      new TimerTask() {
        @Override
        public void run() {
          Logger log = Logger.getLogger(Balancer.class.getName());

          timerPass = timerPass + 1;
          log.info(String.valueOf(timerPass));

          if ((timerPass * 1000) >= TIMEOUT_LB_REPLY) {
            lastTransaction = null;
            timerTaskGateWay.cancel();
            log.info("TIME'S UP");
          }
        }
      };

    timerPass = 0;
    Timer timer = new Timer();
    timer.scheduleAtFixedRate(timerTaskGateWay, 1000, 1000);
  }

  private void executeTimeOutLB() {
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
            log.info("TIME'S UP");
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
      this.log.info("Load Balancer - Error commit transaction.");
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
      this.log.info("Load Balancer - Error commit transaction.");
      ie.printStackTrace();
    }
  }

  private String buildSource() {
    return new StringBuilder(this.groupManager.getGroup())
      .append("/")
      .append(this.idManager.getIP())
      .toString();
  }

  public void removeFirstDevice(String targetIp) throws MqttException {
    try {
      List<Device> allDevices = deviceManager.getAllDevices();

      if (!allDevices.isEmpty()) {
        Device deviceARemover = allDevices.get(0);
        JsonObject jsonPublish = new JsonObject();
        jsonPublish.addProperty("id", deviceARemover.getId());
        jsonPublish.addProperty("url", "tcp://" + targetIp);
        jsonPublish.addProperty("port", deviceARemover.getId());
        jsonPublish.addProperty("user", "karaf");
        jsonPublish.addProperty("password", "karaf");

        iPublisher.publish(
          deviceARemover.getId(),
          "SET VALUE brokerMqtt{" + jsonPublish.toString() + "}"
        );

        deviceManager.removeDevice(allDevices.get(0).getId());
      }
    } catch (IOException ioe) {
      this.log.info(
          "Error! To retrieve device list or to remove the first device."
        );
      ioe.printStackTrace();
    }
  }

  public void loadSwapReceberDispositivo(String deviceJSON) {
    try {
      this.log.info("DeviceJSON: " + deviceJSON);

      Device device = DeviceWrapper.toDevice(deviceJSON);
      this.log.info("Device after convert: " + DeviceWrapper.toJSON(device));

      deviceManager.addDevice(device);
    } catch (IOException ioe) {
      this.log.info("Error! To add a new device to the list.");
      ioe.printStackTrace();
    }
  }

  public void stopTimeout() {
    while (!executorTimeout.isShutdown());
  }

  @Override
  public void update(Object object) {
    if (object instanceof String) {
      this.log.info("Load balancer - New message - Valid");

      String hashTransaction = (String) object;

      this.log.info("HashTransaction: " + hashTransaction);

      Transaction transaction =
        this.connector.getTransactionByHash(hashTransaction);

      if (
        transaction != null &&
        System.currentTimeMillis() - transaction.getPublishedAt() < 15000 // Evitar o processamento de mensagens antigas.
      ) {
        if (lastTransaction != null) {
          this.processTransactions(transaction);
        } else {
          this.messageArrived(transaction);
        }
      }
    } else {
      this.log.info("Load balancer - New message - Invalid");
    }
  }

  /* Thread para verificar o IP do client tangle e se inscrever nos tópicos
    de interesse.*/
  @Override
  public void run() {
    try {
      InetAddress inet = InetAddress.getByName(
        this.connector.getLedgerWriter().getUrl()
      );

      if (inet.isReachable(3000)) {
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
      this.log.info("Error! Unknown Host.");
      uhe.printStackTrace();
    } catch (IOException ioe) {
      this.log.info("Error! Can't connect to InetAddress.");
      ioe.printStackTrace();
    }
  }
}

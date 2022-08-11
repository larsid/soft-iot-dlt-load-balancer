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
import java.time.Duration;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.json.JSONArray;

/**
 *
 * @author  Allan Capistrano, Antonio Crispim, Uellington Damasceno
 * @version 0.0.2
 */
public class Balancer implements ILedgerSubscriber {

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

  final Duration timeout = Duration.ofSeconds(40);
  ExecutorService executor = Executors.newSingleThreadExecutor();

  public Balancer(long timeoutLB, long timeoutGateway) {
    this.TIMEOUT_LB_REPLY = timeoutLB;
    this.TIMEOUT_GATEWAY = timeoutGateway;

    this.buildTimerResendTransaction();
    this.resent = 0;
    this.lastStatus = null;
  }

  public void buildTimerTaskLB() {
    timerTaskLb =
      new TimerTask() {
        @Override
        public void run() {
          timerPass = timerPass + 1;
          System.out.println(timerPass);

          if ((timerPass * 1000) >= TIMEOUT_LB_REPLY) {
            lastTransaction = null;
            timerTaskLb.cancel();
            System.out.println("TIME'S UP");
          }
        }
      };
  }

  public void buildTimerResendTransaction() {
    timerTaskGateWay =
      new TimerTask() {
        @Override
        public void run() {
          timerPass = timerPass + 1;
          System.out.println(timerPass);

          if ((timerPass * 1000) >= TIMEOUT_GATEWAY) {
            resendTransaction();
            timerTaskGateWay.cancel();
            System.out.println("TIME'S UP");
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
    System.out.println("Set topics: " + topicsJSON);

    this.subscribedTopics =
      new JSONArray(topicsJSON)
        .toList()
        .stream()
        .filter(String.class::isInstance)
        .map(String.class::cast)
        .collect(toList());
  }

  public void start() {
    this.subscribedTopics.forEach(
        topic -> this.connector.subscribe(topic, this)
      );
  }

  public void stop() {
    this.subscribedTopics.forEach(
        topic -> this.connector.unsubscribe(topic, this)
      );
  }

  private void messageArrived(Transaction transaction) {
    System.out.println("Waiting for LB_ENTRY");
    System.out.println("Load  - Last transaction is Null");

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
        System.out.println("Receive: " + TransactionType.LB_ENTRY);

        // Verificando a própria disponibilidade.
        if (this.lastStatus.getAvaible()) {
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
    System.out.println("processing transaction...");

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
              System.out.println(
                "Load Balancer - Error! Unable to retrieve device list."
              );
              ioe.printStackTrace();
            }
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
              System.out.println(
                "Load Balancer - Error! Unable to remove the first device."
              );
              me.printStackTrace();
            }
          }

          break;
        default:
          System.out.println("Error! Something went wrong.");

          break;
      }
    }
  }

  private void executeTimeOutGateWay() {
    timerTaskGateWay =
      new TimerTask() {
        @Override
        public void run() {
          timerPass = timerPass + 1;
          System.out.println(timerPass);

          if ((timerPass * 1000) >= TIMEOUT_LB_REPLY) {
            lastTransaction = null;
            timerTaskGateWay.cancel();
            System.out.println("TIME'S UP");
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
          timerPass = timerPass + 1;
          System.out.println(timerPass);

          if ((timerPass * 1000) >= TIMEOUT_LB_REPLY) {
            lastTransaction = null;
            timerTaskLb.cancel();
            System.out.println("TIME'S UP");
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
      System.out.println("Load Balancer - Error commit transaction.");
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
      System.out.println("Load Balancer - Error commit transaction.");
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
      System.out.println(
        "Error! To retrieve device list or to remove the first device."
      );
      ioe.printStackTrace();
    }
  }

  public void loadSwapReceberDispositivo(String deviceJSON) {
    try {
      System.out.println("DeviceJSON: " + deviceJSON);

      Device device = DeviceWrapper.toDevice(deviceJSON);
      System.out.println(
        "Device after convert: " + DeviceWrapper.toJSON(device)
      );

      deviceManager.addDevice(device);
    } catch (IOException ioe) {
      System.out.println("Error! To add a new device to the list.");
      ioe.printStackTrace();
    }
  }

  public void stopTimeout() {
    while (!executor.isShutdown());
  }

  @Override
  public void update(Object object) {
    if (object instanceof String) {
      System.out.println("Load balancer - New message - Valid");

      String hashTransaction = (String) object;

      System.out.println("HashTransaction: " + hashTransaction);

      Transaction transaction =
        this.connector.getTransactionByHash(hashTransaction);

      if (transaction != null) {
        if (lastTransaction != null) {
          this.processTransactions(transaction);
        } else {
          this.messageArrived(transaction);
        }
      }
    } else {
      System.out.println("Load balancer - New message - Invalid");
    }
  }
}

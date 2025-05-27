package dlt.load.balancer.model;

import br.uefs.larsid.extended.mapping.devices.services.IDevicePropertiesManager;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import dlt.auth.services.IPublisher;
import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.Request;
import dlt.client.tangle.hornet.model.transactions.Status;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import dlt.id.manager.services.IDLTGroupManager;
import dlt.id.manager.services.IIDManagerService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class BalancerStateTransitionTest {

    // Mocks para todas as dependências externas do Balancer.
    // O Mockito criará implementações falsas para nós.
    @Mock
    private LedgerConnector ledgerConnector;
    @Mock
    private IDevicePropertiesManager deviceManager;
    @Mock
    private IIDManagerService idManager;
    @Mock
    private IDLTGroupManager groupManager;
    @Mock
    private IPublisher publisher;

    // @Spy permite que chamemos os métodos reais do objeto,
    // mas também verifiquemos suas invocações.
    @Spy
    private BalancerConfigs configs;

    // @InjectMocks cria uma instância de Balancer e injeta
    // automaticamente os mocks criados acima.
    @InjectMocks
    private Balancer balancer;

    private final String localSource = "group/127.0.0.1/1883";
    private final String remoteSource = "group/192.168.1.10/1883";
    
    /**
     * Este método é executado antes de cada teste.
     * Usado para configurar o estado inicial comum.
     */
    @BeforeEach
    void setUp() {
        // Configuração padrão para os mocks, para evitar NullPointerExceptions.
        when(groupManager.getGroup()).thenReturn("group");
        when(idManager.getIP()).thenReturn("127.0.0.1");
        when(configs.getMqttPort()).thenReturn("1883");
        when(balancer.buildSource()).thenReturn(localSource);

        // Inicia o Balancer manualmente para que o estado inicial seja IdleState.
        balancer.start();
        
        // Simula o recebimento de um status inicial para que o gateway se considere disponível.
        Status initialStatus = new Status(localSource, "group", false, 10.0, 10.0, true);
        balancer.update(initialStatus, "msgId");
    }

    @Test
    @DisplayName("Deve iniciar no estado Idle")
    void should_startInIdleState() {
        // Acessa o estado interno para verificação.
        // Em um teste, isso é aceitável.
        assertInstanceOf(IdleState.class, balancer.getState(), "O estado inicial do Balancer deve ser IdleState");
    }

    @Test
    @DisplayName("Transição: Idle -> WaitingLBRequestState ao receber LB_ENTRY")
    void should_transitionFromIdleToWaitingLBRequestState_when_receivesLBEntry() throws InterruptedException {
        // Arrange: Prepara a transação que vai disparar a mudança de estado.
        Transaction lbEntry = new Status(localSource, "group", true, 10.0, 10.0, true);
        when(configs.isBalanceable()).thenReturn(true);
        when(configs.validPublishMessageInterval(anyLong())).thenReturn(true);

        // Act: Executa o método que recebe a transação.
        balancer.update(lbEntry, "msgId");

        // Assert: Verifica se a transição de estado ocorreu corretamente.
        assertInstanceOf(WaitingLBRequestState.class, balancer.getState(), "Estado deveria ser WaitingLBRequestState");
        
        // Verifica se uma transação de resposta (LBReply) foi enviada.
        ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
        verify(ledgerConnector, times(1)).put(transactionCaptor.capture());
        assertEquals(TransactionType.LB_REPLY, transactionCaptor.getValue().getType(), "Deveria ter enviado um LB_REPLY");
    }

    @Test
    @DisplayName("Transição: WaitingLBRequestState -> Idle ao receber LB_REQUEST")
    void should_transitionFromWaitingRequestToIdle_when_receivesLBRequest() throws IOException, InterruptedException {
        // Arrange: Coloca o balancer no estado WaitingLBRequestState.
        balancer.transitionTo(new WaitingLBRequestState(balancer, false));
        
        String deviceJson = "{\"id\":\"sensor1\"}";
        Transaction lbRequest = new Request(remoteSource, "group", deviceJson, localSource);
        when(configs.isBalanceable()).thenReturn(true);
        when(configs.validPublishMessageInterval(anyLong())).thenReturn(true);
        
        // Act: Simula o recebimento do LB_REQUEST.
        balancer.update(lbRequest, "msgId");
        
        // Assert: Verifica se o estado voltou para Idle.
        assertInstanceOf(IdleState.class, balancer.getState(), "Estado deveria voltar para IdleState");

        // Verifica se o dispositivo foi adicionado e se uma resposta final foi enviada.
        verify(deviceManager, times(1)).addDevice(any(Device.class));
        ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
        verify(ledgerConnector, times(1)).put(transactionCaptor.capture());
        assertEquals(TransactionType.LB_REPLY, transactionCaptor.getValue().getType(), "Deveria ter enviado um REPLY final");
    }
    
    @Test
    @DisplayName("Transição: Idle -> WaitingLBReplyState ao receber LB_ENTRY (Loopback)")
    void should_transitionFromIdleToWaitingReplyState_when_receivesLoopbackLBEntry() throws InterruptedException {
        // Arrange: Cria uma transação de loopback (mesma origem do balancer).
        Transaction lbEntryLoopback = new Status(localSource, "group", true, 10.0, 10.0, true);
        when(configs.isBalanceable()).thenReturn(true);
        when(configs.validPublishMessageInterval(anyLong())).thenReturn(true);

        // Act: Envia a transação de loopback.
        balancer.update(lbEntryLoopback, "msgId");

        // Assert: Verifica a transição para WaitingLBReplyState.
        assertInstanceOf(WaitingLBReplyState.class, balancer.getState(), "Estado deveria ser WaitingLBReplyState");
        
        // Em caso de loopback, nenhuma transação deve ser enviada imediatamente.
        verify(ledgerConnector, never()).put(any(Transaction.class));
    }

    /*@Test
    @DisplayName("Transição: WaitingLBReply -> ProcessSingleLayerSendDevice ao receber LB_ENTRY_REPLY")
    void should_transitionFromWaitingReplyToSendDevice_when_receivesEntryReply() throws IOException, InterruptedException {
        // Arrange: Coloca o balancer no estado correto e prepara as dependências.
        balancer.transitionTo(new WaitingLBReplyState(balancer));
        
        // Prepara um dispositivo para ser "removido".
        Device deviceToSend = new Device();
        deviceToSend.setId("temp-sensor-01");
        when(deviceManager.getAllDevices()).thenReturn(Collections.singletonList(deviceToSend));
        
        Transaction lbEntryReply = new LBEntryReply(remoteSource, "group", localSource);
        when(configs.isBalanceable()).thenReturn(true);
        when(configs.validPublishMessageInterval(anyLong())).thenReturn(true);
        
        // Act: Envia a resposta de outro gateway.
        balancer.update(lbEntryReply, "msgId");
        
        // Assert: O estado agora deve ser o de esperar a confirmação do recebimento do dispositivo.
        assertInstanceOf(WaitingLBDeviceRecivedReplyState.class, balancer.getState(), "Estado deveria ser WaitingLBDeviceRecivedReplyState");
        
        // Verifica se a transação com o dispositivo foi enviada.
        ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
        verify(ledgerConnector, times(1)).put(transactionCaptor.capture());
        assertEquals(TransactionType.LB_REQUEST, transactionCaptor.getValue().getType(), "Deveria ter enviado um REQUEST com o dispositivo");
    }*/
}
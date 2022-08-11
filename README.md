# soft-iot-dlt-load-balancer
O `soft-iot-dlt-load-balancer` é o *bundle* que executa o algoritmo de balanceamento de carga no *gateway* quando o mesmo está sobrecarregado.

## Instalação

Para instalar o `soft-iot-dlt-load-balancer` é necessário [configurar o repositório fonte](https://github.com/larsid/soft-iot-dlt-architecture#repositório-fonte) e em seguida executar o seguinte comando no terminal do servicemix.

    bundle:install mvn:com.github.larsid/soft-iot-dlt-load-monitor/master

O `soft-iot-dlt-load-balancer` faz uso de serviços dos *bundles* `soft-iot-mapping-devices`, `SOFT-IoT-DLT-Auth`, `soft-iot-dlt-id-manager`, `soft-iot-dlt-client-tangle`, `soft-iot-dlt-load-monitor` e por isso os mesmos devem estar instalados e executando para que o `soft-iot-dlt-load-balancer` possa iniciar.

## Configuração

| Propriedade | Descrição | Valor padrão
|-------------|-----------|-------------
| TIMEOUT_LB_REPLY | Tempo máximo de espera em milissegundos para receber a resposta de um *gateway*. | 20000
| TIMEOUT_GATEWAY | Tempo máximo de espera em milissegundos para o reenvio da última transação. | 40000
| TOPICS | Lista de tópicos utilizados para ler as transações. | [sn, tx]

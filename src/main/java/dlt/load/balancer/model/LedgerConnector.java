package dlt.load.balancer.model;

import dlt.client.tangle.hornet.model.transactions.IndexTransaction;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import dlt.client.tangle.hornet.services.ILedgerReader;
import dlt.client.tangle.hornet.services.ILedgerSubscriber;
import dlt.client.tangle.hornet.services.ILedgerWriter;

/**
 *
 * @author  Antonio Crispim, Uellington Damasceno
 * @version 0.0.1
 */
public class LedgerConnector {

    private ILedgerReader ledgerReader;
    private ILedgerWriter ledgerWriter;

    public void setLedgerWriter(ILedgerWriter ledgerWriter) {
        this.ledgerWriter = ledgerWriter;
    }

    public void setLedgerReader(ILedgerReader ledgerReader) {
        this.ledgerReader = ledgerReader;
    }

    public void subscribe(String topic, ILedgerSubscriber iLedgerSubscriber) {
        this.ledgerReader.subscribe(topic, iLedgerSubscriber);
    }

    public void unsubscribe(String topic, ILedgerSubscriber iLedgerSubscriber) {
        this.ledgerReader.unsubscribe(topic, iLedgerSubscriber);
    }

    public void put(Transaction transaction) throws InterruptedException {
        IndexTransaction indexedTransaction = new IndexTransaction(transaction.getType().name(), transaction);
        this.ledgerWriter.put(indexedTransaction);
    }

    public ILedgerWriter getLedgerWriter() {
      return ledgerWriter;
    }
}

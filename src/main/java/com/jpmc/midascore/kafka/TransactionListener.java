package com.jpmc.midascore.kafka;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    private final DatabaseConduit databaseConduit;

    public TransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(
        topics = "${general.kafka-topic}",
        groupId = "midas-core-group"
    )
    public void listen(Transaction transaction) {
        // Validate transaction
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        // Check if sender and recipient exist
        UserRecord sender = databaseConduit.findUserById(senderId);
        UserRecord recipient = databaseConduit.findUserById(recipientId);

        if (sender == null || recipient == null) {
            // Invalid sender or recipient ID
            return;
        }

        // Check if sender has sufficient balance
        if (sender.getBalance() < amount) {
            // Insufficient balance
            return;
        }

        // Transaction is valid - persist it and update balances
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount);
        databaseConduit.save(transactionRecord);

        // Update balances
        float newSenderBalance = sender.getBalance() - amount;
        float newRecipientBalance = recipient.getBalance() + amount;

        databaseConduit.updateUserBalance(sender, newSenderBalance);
        databaseConduit.updateUserBalance(recipient, newRecipientBalance);
    }
}

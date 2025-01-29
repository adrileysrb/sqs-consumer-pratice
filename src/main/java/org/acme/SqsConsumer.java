package org.acme;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

@ApplicationScoped
public class SqsConsumer {

    private final SqsClient sqsClient;

    @ConfigProperty(name = "quarkus.sqs.queue.url")
    String queueUrl;

    public SqsConsumer(SqsClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    // Consumir mensagens periodicamente
    //@Scheduled(every = "3s")  // Chama o método a cada 30 segundos
    public void receiveMessages() {
        System.out.println("Verificar mensagens...");
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .maxNumberOfMessages(5)
            .waitTimeSeconds(20)
            .build();

        ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);

        List<Message> messages = response.messages();
        for (Message message : messages) {
            // Processar a mensagem
            System.out.println("Mensagem recebida: " + message.body());

            // Remover a mensagem após o processamento
            deleteMessage(message.receiptHandle());
        }
    }

    private void deleteMessage(String receiptHandle) {
        sqsClient.deleteMessage(deleteMessageRequest -> deleteMessageRequest
            .queueUrl(queueUrl)
            .receiptHandle(receiptHandle)
        );
    }
}


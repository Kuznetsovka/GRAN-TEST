package ru.gransoft.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.gransoft.kafka.dto.DocumentDto;

import java.util.concurrent.ThreadLocalRandom;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaProducer {

    @Value("${kafka.post.topic}")
    private String topic;

    private int messageNumber = 0;

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public void sendMessages() {

        while (messageNumber != 10_000) {
            messageNumber++;
            StringBuilder sf = new StringBuilder();
            DocumentDto dto = DocumentDto.builder()
                            .seq(messageNumber)
                            .text(String.valueOf(sf.append("Запрос № ").append(messageNumber)))
                            .parentId(messageNumber == 1L ? null : (long) (messageNumber - 1))
                            .build();
            kafkaTemplate.send(topic, String.valueOf(ThreadLocalRandom.current().nextLong()), dto);
            log.info("Отправлено сообщение номер {}", messageNumber);
        }
    }
}
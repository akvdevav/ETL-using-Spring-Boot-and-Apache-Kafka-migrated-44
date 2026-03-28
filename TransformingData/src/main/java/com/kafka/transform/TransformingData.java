package com.kafka.transform;

import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TransformingData {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @RabbitListener(queuesToDeclare = @Queue(name = "source_topic"))
    public void handleMessage(String message) {
        int res = 0;
        String[] fileData = message.split(",");
        if (fileData.length >= 3) {
            String operator = fileData[2];
            try {
                int op1 = Integer.parseInt(fileData[0].trim());
                int op2 = Integer.parseInt(fileData[1].trim());
                switch (operator) {
                    case "+":
                        res = op1 + op2;
                        break;
                    case "-":
                        res = op1 - op2;
                        break;
                    case "*":
                        res = op1 * op2;
                        break;
                    case "/":
                        res = op2 != 0 ? op1 / op2 : 0;
                        break;
                    default:
                        // Unknown operator; result remains 0
                        break;
                }
            } catch (NumberFormatException e) {
                // Parsing error; result remains 0
            }
        }
        String result = message + "," + res;
        rabbitTemplate.convertAndSend("target_topic", result);
    }

    public static void main(String[] args) {
        SpringApplication.run(TransformingData.class, args);
    }
}
package com.example.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.Queue;
import com.example.demo.service.LoadingService;

@Controller
public class LoadingController {

    @Autowired
    private LoadingService service;

    @RabbitListener(queuesToDeclare = @Queue(name = "target_topic"))
    public void listen(String message) {
        System.out.println(message);
        service.loadDataToDB(message);
    }
}
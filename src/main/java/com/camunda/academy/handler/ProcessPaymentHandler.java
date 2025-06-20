package com.camunda.academy.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.camunda.academy.services.TrackingOrderService;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;

public class ProcessPaymentHandler implements JobHandler {

    private static final Logger logger = LoggerFactory.getLogger(ProcessPaymentHandler.class);

    private final TrackingOrderService trackingOrderService = new TrackingOrderService();

    @Override
    public void handle(JobClient client, ActivatedJob job) throws Exception {
        final Map<String, Object> inputVariables = job.getVariablesAsMap();
        final String orderId = (String) inputVariables.get("orderId");
        logger.info("Order: {} Processing payment", orderId);
        final String paymentConfirmation = trackingOrderService.processPayment(job);
        logger.info("Order: {} Payment processed successfully with confirmation: {}", orderId, paymentConfirmation);
        logger.info("Process variables retrieved from processPaymentsHandler: {}", inputVariables);
        inputVariables.put("paymentConfirmation", paymentConfirmation);
        logger.info("Order: {} Payment processed successfully", orderId);
        client.newCompleteCommand(job.getKey())
                .variables(inputVariables)
                .send()
                .join();
        

    }
}

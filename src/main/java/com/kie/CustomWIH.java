package com.kie;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemHandler;
import org.kie.api.runtime.process.WorkItemManager;

import java.util.HashMap;
import java.util.Map;


public class CustomWIH implements WorkItemHandler {




    private static final Logger logger = LoggerFactory.getLogger(CustomWIH.class);

    private String connectionFactoryName;
    private String destinationName;
    private KieSession kieSession;
    Map<String, Object> map = new HashMap<>();

    private ConnectionFactory connectionFactory;
    private Destination destination;



    public CustomWIH(String connectionFactoryName,
                     String destinationName, KieSession kieSession) {
        System.out.println("in constructor");

        this.connectionFactoryName = connectionFactoryName;
        this.destinationName = destinationName;
        System.out.println("Factory : " + connectionFactoryName);
        System.out.println("Destination : " + destinationName);
        this.kieSession = kieSession;
        System.out.println("Created session ");
        init();
        System.out.println("finished constructor");
    }



    protected void init() {
        try {

            System.out.println("in init");

            InitialContext ctx = new InitialContext();
            this.connectionFactory = (ConnectionFactory) ctx.lookup(connectionFactoryName);
            this.destination = (Destination) ctx.lookup(destinationName);

            logger.info("JMS based work item handler successfully activated on destination {}",
                    destination);
            System.out.println("finished init");
        } catch (Exception e) {
            logger.error("Unable to initialize JMS  Receive work item handler due to {}",
                    e.getMessage(),
                    e);
        }
    }

    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {

            if (connectionFactory == null || destination == null) {
            throw new RuntimeException("Connection factory and destination must be set for JMS Receive  task handler");
        }


        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;
        Message message = null;
        try {
            System.out.println("in  executeWorkItem");
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            System.out.println("Created Connection and session ");
            consumer = session.createConsumer(destination);
            System.out.println("Created consumer ");
            System.out.println("consuming ");
            message = consumer.receive(5000); // Wait for up to 5 second
            System.out.println("consumed ");
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String receivedMessage = textMessage.getText();
                System.out.println("Received message: " + receivedMessage);
                System.out.println("Starting Process Activition");
                startJBPMProcess(receivedMessage);
                System.out.println("Finished Process Activition");
                manager.completeWorkItem(workItem.getId(),
                        map);


            } else {
                System.out.println("No message received within the timeout.");
                if(message!=null)
                {
                    System.out.println("it is not null but not a text message");
                }
                manager.completeWorkItem(workItem.getId(), null);
            }




        } catch (Exception e) {
            System.out.println("Encountered Exception");
            System.out.println(e.getMessage());
            manager.completeWorkItem(workItem.getId(),map);

        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (JMSException e) {
                    logger.warn("Error when closing consumer",
                            e);
                }
            }

            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    logger.warn("Error when closing queue session",
                            e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    logger.warn("Error when closing queue connection",
                            e);
                }
            }
        }

        manager.completeWorkItem(workItem.getId(),map);

    }


    private void startJBPMProcess(String messageContent) {


        System.out.println("in  startJBPMProcess");
        logger.info("Splitting message");

        Map<String, Object> params = new HashMap<String, Object>();

        ObjectMapper objectMapper = new ObjectMapper();

       try {
           Map<String, String> message = objectMapper.readValue(messageContent, Map.class);

           // Access fields easily
           String sender = message.get("sender");
           String time = message.get("time");
           String process = message.get("process");
           String data = message.get("data");


           System.out.println("Decoded Message:");
           System.out.println("Sender: " + sender);
           System.out.println("Time: " + time);
           System.out.println("Process: " + process);
           System.out.println("Data: " + data);

           logger.info("Decoded message");
           // Create parameters map for the process

           System.out.println("setting params ");
           params.put("Sender", sender);
           params.put("Time", time);
           params.put("Process", process);
           params.put("Data", data);
           logger.info("Set message params");
           System.out.println("Starting process with params: " + params);
           kieSession.startProcess(process, params);
           System.out.println("Finished starting process" );
       }catch (Exception e) {
           System.out.println("Encountered Exception : "+ e.getMessage());
       }
        // Start the process


}


    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {
        // Handle abort scenario if needed
        logger.warn("Aborting work item: {}", workItem.getId());
    }
}

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
    Map<String, Object> map = new HashMap<>();

    private ConnectionFactory connectionFactory;
    private Destination destination;

    private boolean transacted = false;

    public CustomWIH() {
        this.connectionFactoryName = "java:/JmsXA";
        this.destinationName = "queue/KIE.SIGNAL";
        init();
    }

    public CustomWIH(String connectionFactoryName,
                                         String destinationName) {
        System.out.println("in constructor");

        this.connectionFactoryName = connectionFactoryName;
        this.destinationName = destinationName;
        System.out.println("Factory : " + connectionFactoryName);
        System.out.println("Destination : " + destinationName);
        init();
        System.out.println("finished constructor");
    }



    protected void init() {
        try {
            System.out.println("in init");
            logger.info("in init");

            InitialContext ctx = new InitialContext();
            if (this.connectionFactory == null) {
                this.connectionFactory = (ConnectionFactory) ctx.lookup(connectionFactoryName);
            }
            if (this.destination == null) {
                this.destination = (Destination) ctx.lookup(destinationName);
            }
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
            System.out.println("message "+message);
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String receivedMessage = textMessage.getText();
                System.out.println("Received message: " + receivedMessage);

                System.out.println("consumed ");

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

    public void close() {
        connectionFactory = null;
        destination = null;

    }


    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {
        // Handle abort scenario if needed
        logger.warn("Aborting work item: {}", workItem.getId());
    }
}

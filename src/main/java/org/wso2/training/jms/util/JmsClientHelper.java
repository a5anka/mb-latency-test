package org.wso2.training.jms.util;

import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JmsClientHelper {
    /**
     * Full qualified class name of the andes initial context factory
     */
    public static final String ANDES_INITIAL_CONTEXT_FACTORY = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";

    /**
     * Queue connection factory name used
     */
    public static final String QUEUE_CONNECTION_FACTORY = "andesQueueConnectionfactory";

    /**
     * Topic connection factory name used
     */
    public static final String TOPIC_CONNECTION_FACTORY = "andesTopicConnectionfactory";

    /**
     * Create a inital context with the given parameters
     *
     * @param username   Username
     * @param password   Password
     * @param brokerHost Hostname or IP address of the broker
     * @param port       Port used for AMQP transport
     * @param queueName  Queue name
     * @return InitialContext
     * @throws NamingException
     */
    public static InitialContext getInitialContextForQueue(String username,
            String password,
            String brokerHost,
            String port,
            String queueName) throws NamingException {

        Properties contextProperties = new Properties();
        contextProperties.put(Context.INITIAL_CONTEXT_FACTORY, ANDES_INITIAL_CONTEXT_FACTORY);
        String connectionString = getBrokerConnectionString(username, password, brokerHost, port);
        contextProperties.put("connectionfactory." + QUEUE_CONNECTION_FACTORY, connectionString);
        contextProperties.put("queue." + queueName, queueName);

        return new InitialContext(contextProperties);
    }

    /**
     * Create a inital context with the given parameters
     *
     * @param username   Username
     * @param password   Password
     * @param brokerHost Hostname or IP address of the broker
     * @param port       Port used for AMQP transport
     * @param queueName  Queue name
     * @return InitialContext
     * @throws NamingException
     */
    public static InitialContext getInitialContextForQueueWithoutClientId(String username,
            String password,
            String brokerHost,
            String port,
            String queueName) throws NamingException {

        Properties contextProperties = new Properties();
        contextProperties.put(Context.INITIAL_CONTEXT_FACTORY, ANDES_INITIAL_CONTEXT_FACTORY);
        String connectionString = getBrokerConnectionStringWithoutClientId(username, password, brokerHost, port);
        contextProperties.put("connectionfactory." + QUEUE_CONNECTION_FACTORY, connectionString);
        contextProperties.put("queue." + queueName, queueName);

        return new InitialContext(contextProperties);
    }

    /**
     * Create a inital context with the given parameters
     *
     * @param username   Username
     * @param password   Password
     * @param brokerHost Hostname or IP address of the broker
     * @param port       Port used for AMQP transport
     * @param queueName  Queue name
     * @return InitialContext
     * @throws NamingException
     */
    public static InitialContext getInitialContextForQueueWithFailover(String username,
            String password,
            String brokerHost1,
            String port1,
            String brokerHost2,
            String port2,
            String queueName) throws NamingException {

        Properties contextProperties = new Properties();
        contextProperties.put(Context.INITIAL_CONTEXT_FACTORY, ANDES_INITIAL_CONTEXT_FACTORY);
        String connectionString = getFailoverBrokerConnectionString(username,
                                                                    password,
                                                                    brokerHost1,
                                                                    port1,
                                                                    brokerHost2,
                                                                    port2);
        contextProperties.put("connectionfactory." + QUEUE_CONNECTION_FACTORY, connectionString);
        contextProperties.put("queue." + queueName, queueName);

        return new InitialContext(contextProperties);
    }

    /**
     * Create a inital context with the given parameters
     *
     * @param username   Username
     * @param password   Password
     * @param brokerHost Hostname or IP address of the broker
     * @param port       Port used for AMQP transport
     * @param queueName  Queue name
     * @return InitialContext
     * @throws NamingException
     */
    public static InitialContext getInitialContextForQueueSSL(String username,
            String password,
            String brokerHost,
            String port,
            String queueName) throws NamingException {

        Properties contextProperties = new Properties();
        contextProperties.put(Context.INITIAL_CONTEXT_FACTORY, ANDES_INITIAL_CONTEXT_FACTORY);
        String connectionString = getBrokerSSLConnectionString(username, password, brokerHost, port);
        contextProperties.put("connectionfactory." + QUEUE_CONNECTION_FACTORY, connectionString);
        contextProperties.put("queue." + queueName, queueName);

        return new InitialContext(contextProperties);
    }

    /**
     * Create a inital context with the given parameters
     *
     * @param username   Username
     * @param password   Password
     * @param brokerHost Hostname or IP address of the broker
     * @param port       Port used for AMQP transport
     * @param topicName  Topic name
     * @return InitialContext
     * @throws NamingException
     */
    public static InitialContext getInitialContextForTopic(String username,
            String password,
            String brokerHost,
            String port,
            String topicName) throws NamingException {

        Properties contextProperties = new Properties();
        contextProperties.put(Context.INITIAL_CONTEXT_FACTORY, ANDES_INITIAL_CONTEXT_FACTORY);
        String connectionString = getBrokerConnectionString(username, password, brokerHost, port);
        contextProperties.put("connectionfactory." + TOPIC_CONNECTION_FACTORY, connectionString);
        contextProperties.put("topic." + topicName, topicName);

        return new InitialContext(contextProperties);
    }

    public static InitialContext getInitialContextForTopicWithFailover(String username,
            String password,
            String brokerHost1,
            String port1,
            String brokerHost2,
            String port2,
            String topicName) throws NamingException {

        Properties contextProperties = new Properties();
        contextProperties.put(Context.INITIAL_CONTEXT_FACTORY, ANDES_INITIAL_CONTEXT_FACTORY);
        String connectionString = getFailoverBrokerConnectionString(username,
                                                                    password,
                                                                    brokerHost1,
                                                                    port1,
                                                                    brokerHost2,
                                                                    port2);
        contextProperties.put("connectionfactory." + TOPIC_CONNECTION_FACTORY, connectionString);
        contextProperties.put("topic." + topicName, topicName);

        return new InitialContext(contextProperties);
    }

    /**
     * Generate broker connection string
     *
     * @param userName   Username
     * @param password   Password
     * @param brokerHost Hostname of broker (E.g. localhost)
     * @param port       Port (E.g. 5672)
     * @return Broker Connection String
     */
    private static String getBrokerConnectionString(String userName, String password,
            String brokerHost, String port) {

        return "amqp://" + userName + ":" + password + "@clientID/carbon?brokerlist='tcp://"
                + brokerHost + ":" + port + "'";
    }

    /**
     * Generate broker connection string
     *
     * @param userName   Username
     * @param password   Password
     * @param brokerHost Hostname of broker (E.g. localhost)
     * @param port       Port (E.g. 5672)
     * @return Broker Connection String
     */
    private static String getBrokerConnectionStringWithoutClientId(String userName, String password,
            String brokerHost, String port) {

        return "amqp://" + userName + ":" + password + "@/carbon?brokerlist='tcp://"
                + brokerHost + ":" + port + "'";
    }

    /**
     * Generate broker connection string with failover
     *
     * @param userName   Username
     * @param password   Password
     * @param brokerHost Hostname of broker (E.g. localhost)
     * @param port       Port (E.g. 5672)
     * @return Broker Connection String
     */
    private static String getFailoverBrokerConnectionString(String userName, String password,
            String brokerHost1, String port1, String brokerHost2, String port2) {

        return "amqp://" + userName + ":" + password + "@clientID/carbon?brokerlist='"
                + "tcp://" + brokerHost1 + ":" + port1 + "?connectdelay='1000'&connecttimeout='3000'&retries='3';"
                + "tcp://" + brokerHost2 + ":" + port2 + "?connectdelay='1000'&connecttimeout='3000'&retries='3';"
                + "'";
    }

    /**
     * Generate broker SSL connection string
     *
     * @param userName   Username
     * @param password   Password
     * @param brokerHost Hostname of broker (E.g. localhost)
     * @param port       Port (E.g. 5672)
     * @return Broker Connection String
     */
    private static String getBrokerSSLConnectionString(String userName, String password,
            String brokerHost, String port) {

        return "amqp://" + userName + ":" + password + "@clientID/carbon?brokerlist='tcp://"
                + brokerHost + ":" + port
                + "?ssl='true'&trust_store='conf/sample-client-truststore"
                + ".jks'&trust_store_password='user123'&key_store='conf/clientkeystore.jks'&key_store_password='user123''";
    }
}

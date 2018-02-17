package org.wso2.training.jms.sample;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.wso2.training.jms.util.JmsClientHelper;

import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class SampleConsumer {

    /**
     * args: number_of_messages
     */
    public static void main(String[] args) throws NamingException, JMSException {

        int messageCount = Integer.parseInt(args[0]);

        String topicName = "MyTopic";
        InitialContext initialContext = JmsClientHelper.getInitialContextForTopic("admin",
                                                                                  "admin",
                                                                                  "localhost",
                                                                                  "5672",
                                                                                  topicName);

        TopicConnectionFactory connectionFactory = (TopicConnectionFactory) initialContext.lookup(JmsClientHelper
                                                                                       .TOPIC_CONNECTION_FACTORY);
        Topic destination = (Topic) initialContext.lookup(topicName);

        TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();
        TopicSession session = connection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        TopicSubscriber receiver = session.createSubscriber(destination);

        System.out.println("Topic subscriber initialized");

        Histogram histogram = initMetrics();

        long endTime;
        long startTime;
        for (int i = 0; i < messageCount; i++) {
            TextMessage receive = (TextMessage) receiver.receive();

            endTime = System.currentTimeMillis();
            startTime = Long.parseLong(receive.getText());
            histogram.update(endTime - startTime);
        }

        connection.close();
    }

    private static Histogram initMetrics() {
        MetricRegistry registry = new MetricRegistry();
        Histogram histogram = registry.histogram("message/latency");

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                                                        .convertRatesTo(TimeUnit.SECONDS)
                                                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                        .build();
        reporter.start(20, TimeUnit.SECONDS);
        return histogram;
    }
}

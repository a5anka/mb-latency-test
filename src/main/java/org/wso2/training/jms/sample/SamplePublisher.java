package org.wso2.training.jms.sample;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.RateLimiter;
import org.wso2.training.jms.util.JmsClientHelper;

import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.QueueSession;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class SamplePublisher {

    private static ConsoleReporter reporter;

    /**
     * args: number_of_messages publish_rate
     */
    public static void main(String[] args) throws NamingException, JMSException {
        int numberOfMessages = Integer.parseInt(args[0]);
        int publishRate = Integer.parseInt(args[1]);

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

        TopicPublisher sender = session.createPublisher(destination);

        System.out.println("Topic publisher initialized");

        Meter meter = initMetrics();
        RateLimiter r = RateLimiter.create(publishRate);
        for (int i = 0; i < numberOfMessages; i++) {
            r.acquire();
            sender.publish(session.createTextMessage(String.valueOf(System.currentTimeMillis())));
            meter.mark();
        }

        System.out.println("Message published to topic");
        reporter.report();
        connection.close();
    }

    private static Meter initMetrics() {
        MetricRegistry registry = new MetricRegistry();
        Meter histogram = registry.meter("message.publish.rate");

        reporter = ConsoleReporter.forRegistry(registry)
                                  .convertRatesTo(TimeUnit.SECONDS)
                                  .convertDurationsTo(TimeUnit.MILLISECONDS)
                                  .build();
        reporter.start(20, TimeUnit.SECONDS);
        return histogram;
    }
}

package se.ivankrizsan.springdata.dynamodb.demo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * Persistence configuration used by tests.
 *
 * @author Ivan Krizsan
 */
@Configuration
public class PersistenceTestConfiguration {
    /* Constant(s): */
    private static final Logger LOGGER =
        LoggerFactory.getLogger(PersistenceTestConfiguration.class);
    protected final static int DYNAMODB_PORT = 8000;

    /* Dependencies: */


    /**
     * Creates a local DynamoDB instance running in a Docker container.
     * Ensures that the container is started before bean creation completes, in
     * order to be able to obtain the host port on which the DynamoDB instance is available.
     *
     * @return DynamoDB Testcontainers container.
     */
    @Bean
    public GenericContainer dynamoDBContainer() {
        final GenericContainer theDynamoDBContainer =
            new GenericContainer(DockerImageName.parse("amazon/dynamodb-local:latest"))
                .withExposedPorts(DYNAMODB_PORT);
        theDynamoDBContainer.waitingFor(new HostPortWaitStrategy());
        theDynamoDBContainer.start();
        return theDynamoDBContainer;
    }

    /**
     * Creates a DynamoDB client bean for the DynamoDB instance running in the supplied
     * container with the supplied credentials.
     * Must be named "amazonDynamoDB", otherwise Spring Data DynamoDB initialization will fail.
     *
     * @param inDynamoDBCredentials DynamoDB credentials.
     * @param inDynamoDBContainer DynamoDB Testcontainers container.
     * @return DynamoDB client bean.
     */
    @Bean(destroyMethod = "shutdown")
    public AmazonDynamoDB amazonDynamoDB(
        final AWSCredentials inDynamoDBCredentials,
        @Qualifier("dynamoDBContainer") final GenericContainer inDynamoDBContainer) {

        /* Construct the DynamoDB instance URL pointing at the Testcontainers container. */
        final String theDynamoDbEndpoint = "http://"
            + inDynamoDBContainer.getHost()
            + ":"
            + inDynamoDBContainer.getMappedPort(DYNAMODB_PORT);

        LOGGER.info("DynamoDB endpoint URL: {}", theDynamoDbEndpoint);

        final AmazonDynamoDB theDynamoDBClient = AmazonDynamoDBClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(inDynamoDBCredentials))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                theDynamoDbEndpoint,
                ""))
            .build();

        return theDynamoDBClient;
    }

    /**
     * Creates a DynamoDB mapper bean using the supplied DynamoDB client.
     * Note that the bean name must be dynamoDB-DynamoDBMapper in order to
     * override the DynamoDB mapper bean from Spring Data DynamoDB.
     *
     * @param inDynamoDBClient DynamoDB client to be used by mapper.
     * @return DynamoDB mapper bean.
     */
    @Bean(name = "dynamoDB-DynamoDBMapper")
    public DynamoDBMapper dynamoDBMapper(final AmazonDynamoDB inDynamoDBClient) {
        return new DynamoDBMapper(inDynamoDBClient);
    }
}

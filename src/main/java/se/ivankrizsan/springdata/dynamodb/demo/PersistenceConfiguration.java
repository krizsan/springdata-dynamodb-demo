package se.ivankrizsan.springdata.dynamodb.demo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import org.socialsignin.spring.data.dynamodb.repository.config.EnableDynamoDBRepositories;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import se.ivankrizsan.springdata.dynamodb.repositories.CirclesRepository;

/**
 * Persistence configuration.
 *
 * @author Ivan Krizsan
 */
@Configuration
@EnableDynamoDBRepositories(basePackageClasses = CirclesRepository.class)
public class PersistenceConfiguration {
    /* Constant(s): */

    /* Dependencies: */
    @Value(("${amazon.dynamodb.endpoint}"))
    protected String mDynamoDBEndpoint;
    @Value("${amazon.aws.accesskey}")
    protected String mAWSAccessKey;
    @Value("${amazon.aws.secretkey}")
    protected String mAWSSecretKey;
    @Value("${amazon.dynamodb.tablenameprefix}")
    protected String mDynamoDBTableNamePrefix;
    @Value("${amazon.aws.region}")
    protected String mAWSRegion;

    /**
     * Creates a bean containing basic AWS credentials.
     *
     * @return AWS credentials bean.
     */
    @Bean
    public AWSCredentials awsCredentials() {
        return new BasicAWSCredentials(mAWSAccessKey, mAWSSecretKey);
    }

    /**
     * Creates a DynamoDB client bean for the DynamoDB instance with the supplied credentials
     * and being available at the endpoint injected into this configuration.
     * Must be named "amazonDynamoDB", otherwise Spring Data DynamoDB initialization will fail.
     *
     * @param inAWSCredentials AWS credentials.
     * @return DynamoDB client bean.
     */
    @Bean(destroyMethod = "shutdown")
    public AmazonDynamoDB amazonDynamoDB(final AWSCredentials inAWSCredentials) {
        return AmazonDynamoDBClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(inAWSCredentials))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(mDynamoDBEndpoint,
                mAWSRegion))
            .build();
    }

    /**
     * Creates a DynamoDB mapper configuration bean in order to prepend all tables names with
     * an application-specific prefix.
     * Note that the name of the bean has to be dynamoDB-DynamoDBMapperConfig in order to
     * override the DynamoDB mapper configuration bean from Spring Data DynamoDB.
     *
     * @return DynamoDB mapper configuration.
     */
    @Bean(name = "dynamoDB-DynamoDBMapperConfig")
    public DynamoDBMapperConfig dynamoDBMapperConfig() {
        return new DynamoDBMapperConfig.Builder()
            .withTableNameOverride(
                DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix(mDynamoDBTableNamePrefix))
            .build();
    }
}

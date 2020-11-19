package se.ivankrizsan.springdata.dynamodb.demo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import org.apache.commons.collections4.IterableUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import se.ivankrizsan.springdata.dynamodb.domain.Circle;
import se.ivankrizsan.springdata.dynamodb.repositories.CirclesRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Examples of persistence in DynamoDB with Spring Data DynamoDB.
 *
 * @author Ivan Krizsan
 */
@SpringBootTest(classes = {
    PersistenceConfiguration.class,
    PersistenceTestConfiguration.class
})
class DynamoDBPersistenceTests {
    /* Constant(s): */
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBPersistenceTests.class);
    protected final static int CIRCLE_RADIUS = 11;
    protected final static String CIRCLE_COLOUR = "blue";
    protected final static int MANY_CIRCLES_COUNT = 2000;

    /* Instance variable(s): */
    @Autowired
    protected CirclesRepository mCirclesRepository;
    @Autowired
    protected DynamoDBMapper mDynamoDBMapper;
    @Autowired
    protected AmazonDynamoDB mAmazonDynamoDB;

    /**
     * Prepares for each test by creating a table in DynamoDB for {@code Circle} entities
     * and set provisioned throughput for global secondary indexes.
     * In addition, delete all {@code Circle} entities as to ensure that the table is
     * empty before executing a test.
     */
    @BeforeEach
    public void setup() {
        try {
            /* Prepare create table request. */
            final CreateTableRequest theCreateCirclesTableRequest =
                mDynamoDBMapper.generateCreateTableRequest(Circle.class);
            final ProvisionedThroughput theCirclesTableProvisionedThroughput =
                new ProvisionedThroughput(10L, 10L);
            theCreateCirclesTableRequest.setProvisionedThroughput(
                theCirclesTableProvisionedThroughput);

            /* Set provisioned throughput for global secondary indexes, if any. */
            final List<GlobalSecondaryIndex> theCircleTableGlobalSecondaryIndexes =
                theCreateCirclesTableRequest.getGlobalSecondaryIndexes();
            if (theCircleTableGlobalSecondaryIndexes != null && theCircleTableGlobalSecondaryIndexes.size() > 0) {
                theCreateCirclesTableRequest
                    .getGlobalSecondaryIndexes()
                    .forEach(v -> v.setProvisionedThroughput(theCirclesTableProvisionedThroughput));
            }

            /* Create table in which to persist circles. */
            mAmazonDynamoDB.createTable(theCreateCirclesTableRequest);
        } catch (final ResourceInUseException theException) {
            LOGGER.debug("Exception occurred creating table", theException);
        }

        /* Delete any circles remaining from previous tests. */
        mDynamoDBMapper.batchDelete(mCirclesRepository.findAll());
    }

    /**
     * Tests persisting one circle and retrieving all circles.
     * Expected result:
     * Retrieving all circles should yield one single circle.
     * The retrieved circle should be identical to the persisted one.
     */
    @Test
    public void persistOneCircleTest() {
        /* Create a circle to be persisted. */
        final Circle theOriginalCircle = new Circle();
        theOriginalCircle.setRadius(CIRCLE_RADIUS);
        theOriginalCircle.setPosition(12, 14);
        theOriginalCircle.setColour(CIRCLE_COLOUR);

        /* Persist the circle. */
        final Circle theExpectedCircle = mCirclesRepository.save(theOriginalCircle);
        LOGGER.info("Circle last updated time: {}", theExpectedCircle.getLastUpdateTime());

        /* Find all circles in the repository. */
        final List<Circle> theCirclesList = IterableUtils.toList(mCirclesRepository.findAll());

        /* Verify the retrieved circle and some of its properties. */
        Assertions.assertEquals(
            1,
            theCirclesList.size(), "One circle should have been persisted");
        final Circle theFoundCircle = theCirclesList.get(0);
        LOGGER.info("Found circle last updated time: {}", theFoundCircle.getLastUpdateTime());
        Assertions.assertEquals(theExpectedCircle, theFoundCircle, "Circle properties should match");
    }

    /**
     * Tests persisting many circles and retrieving all circles.
     * Expected result:
     * Retrieving all circles should yield the same number of circles as persisted.
     * The retrieved circles should be identical to the persisted ones.
     */
    @Test
    public void persistManyCirclesTest() {
        /* Persist the circles. */
        final Map<Integer, Circle> thePersistedCircles = new HashMap<>();
        for (int i = 1; i < MANY_CIRCLES_COUNT + 1; i++) {
            /* Create a circle to be persisted. */
            final Circle theOriginalCircle = new Circle();
            theOriginalCircle.setPosition(12 + i, 14 + i);
            theOriginalCircle.setColour(CIRCLE_COLOUR);
            theOriginalCircle.setRadius(i);

            final Circle thePersistedCircle = mCirclesRepository.save(theOriginalCircle);

            /* Use the radius as key as each circle has a different radius. */
            thePersistedCircles.put(thePersistedCircle.getRadius(), thePersistedCircle);
        }

        /* Find all circles in the repository. */
        final List<Circle> theFoundCircles = IterableUtils.toList(mCirclesRepository.findAll());

        /* Verify the number of persisted circles. */
        Assertions.assertEquals(
            MANY_CIRCLES_COUNT,
            theFoundCircles.size(), "A lot of circles should have been persisted");

        /* Verify properties of the circles. */
        for (Circle theActualCircle : theFoundCircles) {
            final Circle theExpectedCircle = thePersistedCircles.get(theActualCircle.getRadius());
            Assertions.assertEquals(theExpectedCircle,
                theActualCircle,
                "Circle properties should match");
        }
    }
}

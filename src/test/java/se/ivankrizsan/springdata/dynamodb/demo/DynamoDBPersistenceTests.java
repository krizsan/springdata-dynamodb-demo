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
import se.ivankrizsan.springdata.dynamodb.domain.Rectangle;
import se.ivankrizsan.springdata.dynamodb.repositories.CirclesRepository;
import se.ivankrizsan.springdata.dynamodb.repositories.RectanglesRepository;

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
    protected final static String RECTANGLE_COLOUR = "red";
    protected final static int MANY_CIRCLES_COUNT = 2000;

    /* Instance variable(s): */
    @Autowired
    protected CirclesRepository mCirclesRepository;
    @Autowired
    protected RectanglesRepository mRectanglesRepository;
    @Autowired
    protected DynamoDBMapper mDynamoDBMapper;
    @Autowired
    protected AmazonDynamoDB mAmazonDynamoDB;

    /**
     * Prepares for each test by creating tables in DynamoDB for the different type of entities
     * and set provisioned throughput for global secondary indexes.
     * In addition, delete all entities as to ensure that the table is empty before executing
     * a test.
     */
    @BeforeEach
    public void setup() {
        createDynamoDBTableForEntityType(Circle.class);
        createDynamoDBTableForEntityType(Rectangle.class);

        /* Delete any entities remaining from previous tests. */
        mDynamoDBMapper.batchDelete(mCirclesRepository.findAll());
        mDynamoDBMapper.batchDelete(mRectanglesRepository.findAll());
    }

    /**
     * Creates a DynamoDB table for the supplied entity type.
     *
     * @param inEntityType Entity type for which to create table.
     */
    private void createDynamoDBTableForEntityType(final Class inEntityType) {
        try {
            /* Prepare create entity table request. */
            final CreateTableRequest theCreateTableRequest =
                mDynamoDBMapper.generateCreateTableRequest(inEntityType);
            final ProvisionedThroughput theEntityTableProvisionedThroughput =
                new ProvisionedThroughput(10L, 10L);
            theCreateTableRequest.setProvisionedThroughput(
                theEntityTableProvisionedThroughput);

            /* Set provisioned throughput for global secondary indexes, if any. */
            final List<GlobalSecondaryIndex> theEntityTableGlobalSecondaryIndexes =
                theCreateTableRequest.getGlobalSecondaryIndexes();
            if (theEntityTableGlobalSecondaryIndexes != null && theEntityTableGlobalSecondaryIndexes.size() > 0) {
                theCreateTableRequest
                    .getGlobalSecondaryIndexes()
                    .forEach(v -> v.setProvisionedThroughput(theEntityTableProvisionedThroughput));
            }

            /* Create table in which to persist entities. */
            mAmazonDynamoDB.createTable(theCreateTableRequest);
        } catch (final ResourceInUseException theException) {
            LOGGER.debug("Exception occurred creating table for type " + inEntityType.getSimpleName(), theException);
        }
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
        final Circle theOriginalCircle = createCircle();

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
     * Tests persisting one rectangle and retrieving all rectangles.
     * Expected result:
     * Retrieving all rectangles should yield one single rectangle.
     * The retrieved rectangle should be identical to the persisted one.
     */
    @Test
    public void persistOneRectangleTest() {
        /* Create a rectangle to be persisted. */
        final Rectangle theOriginalRectangle = createRectangle();

        /* Persist the rectangle. */
        final Rectangle theExpectedRectangle = mRectanglesRepository.save(theOriginalRectangle);
        LOGGER.info("Rectangle last updated time: {}", theExpectedRectangle.getLastUpdateTime());

        /* Find all rectangles in the repository. */
        final List<Rectangle> theRectanglesList = IterableUtils.toList(mRectanglesRepository.findAll());

        /* Verify the retrieved rectangle and its properties. */
        Assertions.assertEquals(
            1,
            theRectanglesList.size(), "One rectangle should have been persisted");
        final Rectangle theFoundRectangle = theRectanglesList.get(0);
        LOGGER.info("Found rectangle last updated time: {}", theFoundRectangle.getLastUpdateTime());
        Assertions.assertEquals(theExpectedRectangle, theFoundRectangle, "Rectangle properties should match");
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

    /**
     * Tests persisting one circle and one rectangle and retrieving all the
     * circles and rectangles.
     * Expected result:
     * There should be one circle and one rectangle retrieved.
     * The retrieved entities should be identical to the persisted ones.
     */
    @Test
    public void persistMultipleEntityTypesTest() {
        /* Create and persist one rectangle. */
        final Rectangle theOriginalRectangle = createRectangle();
        final Rectangle theExpectedRectangle = mRectanglesRepository.save(theOriginalRectangle);

        /* Create and persist one circle. */
        final Circle theOriginalCircle = createCircle();
        final Circle theExpectedCircle = mCirclesRepository.save(theOriginalCircle);

        /* Retrieve all circles and rectangles. */
        final List<Circle> theCirclesList = IterableUtils.toList(mCirclesRepository.findAll());
        final List<Rectangle> theRectanglesList = IterableUtils.toList(mRectanglesRepository.findAll());

        /* Verify the retrieved circle and some of its properties. */
        Assertions.assertEquals(
            1,
            theCirclesList.size(), "One circle should have been persisted");
        final Circle theFoundCircle = theCirclesList.get(0);
        Assertions.assertEquals(theExpectedCircle, theFoundCircle, "Circle properties should match");

        /* Verify the retrieved rectangle and its properties. */
        Assertions.assertEquals(
            1,
            theRectanglesList.size(), "One rectangle should have been persisted");
        final Rectangle theFoundRectangle = theRectanglesList.get(0);
        Assertions.assertEquals(theExpectedRectangle, theFoundRectangle, "Rectangle properties should match");

    }

    /**
     * Creates a new rectangle setting its properties.
     *
     * @return A new rectangle.
     */
    protected Rectangle createRectangle() {
        final Rectangle theRectangle = new Rectangle();
        theRectangle.setPosition(15, 17);
        theRectangle.setHeight(20);
        theRectangle.setWidth(40);
        theRectangle.setColour(CIRCLE_COLOUR);
        return theRectangle;
    }

    /**
     * Create a new circle setting its properties.
     *
     * @return A new circle.
     */
    protected Circle createCircle() {
        final Circle theOriginalCircle = new Circle();
        theOriginalCircle.setRadius(CIRCLE_RADIUS);
        theOriginalCircle.setPosition(12, 14);
        theOriginalCircle.setColour(CIRCLE_COLOUR);
        return theOriginalCircle;
    }
}

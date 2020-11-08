package se.ivankrizsan.springdata.dynamodb.demo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import org.apache.commons.collections4.IterableUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import se.ivankrizsan.springdata.dynamodb.domain.Circle;
import se.ivankrizsan.springdata.dynamodb.repositories.CirclesRepository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@SpringBootTest(classes = {
    PersistenceConfiguration.class,
    PersistenceTestConfiguration.class
})
class PersistenceTests {
    /* Constant(s): */
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceTests.class);
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

    @BeforeEach
    public void setup() {
        try {
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

            mAmazonDynamoDB.createTable(theCreateCirclesTableRequest);
        } catch (final ResourceInUseException theException) {
            LOGGER.debug("Exception occurred creating table", theException);
        }

        mDynamoDBMapper.batchDelete(mCirclesRepository.findAll());
    }

    /**
     * Tests persisting and retrieving one circle.
     */
    @Test
    public void persistOneCircleTest() throws InterruptedException {
        /* Create a circle to be persisted. */
        final Circle theOriginalCircle = new Circle();
        theOriginalCircle.setRadius(CIRCLE_RADIUS);
        theOriginalCircle.setPosition(12, 14);
        theOriginalCircle.setColour(CIRCLE_COLOUR);

        /* Persist the circle. */
        final Circle thePersistedCircle = mCirclesRepository.save(theOriginalCircle);
        LOGGER.info("Cirlce last accessed time: {}", thePersistedCircle.getLastAccessedTime());

        Thread.sleep(2000);
        /* Find all circles in the repository. */
        final List<Circle> theCirclesList = IterableUtils.toList(mCirclesRepository.findAll());

        /* Verify the retrieved circle and some of its properties. */
        Assertions.assertEquals(
            1,
            theCirclesList.size(), "One circle should have been persisted");
        final Circle theFoundCircle = theCirclesList.get(0);
        LOGGER.info("Found circle last accessed time: {}", theFoundCircle.getLastAccessedTime());
        Assertions.assertEquals(
            CIRCLE_COLOUR,
            theFoundCircle.getColour(), "The colour of the retrieved circle should match");
        Assertions.assertEquals(
            CIRCLE_RADIUS,
            theFoundCircle.getRadius(), "The radius of the retrieved circle should match");
        Assertions.assertNotNull(
            theFoundCircle.getId(), "The retrieved circle should have been assigned an id");
    }

    @Test
    public void persistManyCirclesTest() {
        final String theCacheKey = UUID.randomUUID().toString();
        /* Persist the circles. */
        for (int i = 1; i < MANY_CIRCLES_COUNT + 1; i++) {
            /* Create a circle to be persisted. */
            final Circle theOriginalCircle = new Circle();
            theOriginalCircle.setPosition(12, 14);
            theOriginalCircle.setColour(CIRCLE_COLOUR);
            theOriginalCircle.setRadius(i);

            mCirclesRepository.save(theOriginalCircle);
        }

        /* Find all circles in the repository. */
        final List<Circle> theCirclesList = IterableUtils.toList(mCirclesRepository.findAll());

        /* Verify the number of persisted circles. */
        Assertions.assertEquals(
            MANY_CIRCLES_COUNT,
            theCirclesList.size(), "A lot of circles should have been persisted");
    }

    private static ByteBuffer compressString(final String inStringToCompress) throws IOException {
        return compressBytes(inStringToCompress.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteBuffer compressBytes(final byte[] inBytesToCompress) throws IOException {
        final byte[] theCompressedBytes;
        try (
            ByteArrayOutputStream theByteArrayOutputStream = new ByteArrayOutputStream();
            GZIPOutputStream theGzipOutputStream = new GZIPOutputStream(theByteArrayOutputStream)) {
            theGzipOutputStream.write(inBytesToCompress);
            theGzipOutputStream.finish();
            theCompressedBytes = theByteArrayOutputStream.toByteArray();
        }

        final ByteBuffer theCompressedDataByteBuffer =
            ByteBuffer.allocate(theCompressedBytes.length);
        theCompressedDataByteBuffer.put(theCompressedBytes, 0, theCompressedBytes.length);
        theCompressedDataByteBuffer.position(0);
        return theCompressedDataByteBuffer;
    }

    private static String uncompressString(ByteBuffer inCompressedDataBuffer) throws IOException {
        return new String(uncompressBytes(inCompressedDataBuffer), StandardCharsets.UTF_8);
    }

    private static byte[] uncompressBytes(final ByteBuffer inCompressedDataBuffer)
        throws IOException {
        return uncompressBytes(inCompressedDataBuffer.array());
    }

    private static byte[] uncompressBytes(final byte[] inCompressedData) throws IOException {
        try (
            ByteArrayInputStream theCompressedDataInputStream = new ByteArrayInputStream(
                inCompressedData);
            ByteArrayOutputStream theUncompressedDataOutputStream = new ByteArrayOutputStream();
            GZIPInputStream theGzipInputStream = new GZIPInputStream(theCompressedDataInputStream)) {

            int theChunkSize = 1024;
            byte[] theBuffer = new byte[theChunkSize];
            int theUncompressedDataLength;
            while ((theUncompressedDataLength = theGzipInputStream.read(theBuffer, 0, theChunkSize))
                != -1) {
                theUncompressedDataOutputStream.write(theBuffer, 0, theUncompressedDataLength);
            }

            return theUncompressedDataOutputStream.toByteArray();
        }
    }
}

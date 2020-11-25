package se.ivankrizsan.springdata.dynamodb.domain;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a circle shape with a given radius.
 *
 * @author Ivan Krizsan
 */
@Data
@NoArgsConstructor
@DynamoDBTable(tableName = "Circles")
public class Circle extends Shape {
    /* Constant(s): */
    public static final int DEFAULT_RADIUS = 10;

    /* Instance variable(s): */
    /** Circle radius. */
    @DynamoDBAttribute
    protected int radius = DEFAULT_RADIUS;
}

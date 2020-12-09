package se.ivankrizsan.springdata.dynamodb.domain;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Represents a circle shape with a given radius.
 *
 * @author Ivan Krizsan
 */
@Data
@NoArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@DynamoDBTable(tableName = "circles")
public class Circle extends Shape {
    /* Constant(s): */
    public static final int DEFAULT_RADIUS = 10;

    /* Instance variable(s): */
    /** Circle radius. */
    @DynamoDBAttribute
    protected int radius = DEFAULT_RADIUS;
}

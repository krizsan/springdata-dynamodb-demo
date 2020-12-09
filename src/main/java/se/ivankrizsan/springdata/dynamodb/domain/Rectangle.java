package se.ivankrizsan.springdata.dynamodb.domain;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Represents a rectangle shape with a given height and width.
 *
 * @author Ivan Krizsan
 */
@Data
@NoArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@DynamoDBTable(tableName = "rectangles")
public class Rectangle extends Shape {
    /* Constant(s): */
    public static final int DEFAULT_WIDTH = 10;
    public static final int DEFAULT_HEIGHT = 10;

    /* Instance variable(s): */
    /** Rectangle height. */
    @DynamoDBAttribute
    protected int height = DEFAULT_HEIGHT;
    /** Rectangle width. */
    @DynamoDBAttribute
    protected int width = DEFAULT_WIDTH;
}

package se.ivankrizsan.springdata.dynamodb.domain;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Abstract base class for shape entities located at a position in a two-dimensional
 * coordinate system and that has a colour.
 * Note that since the parent class is annotated with @DynamoDBTable, this class
 * does not need to be annotated with this annotation since it is inherited.
 *
 * @author Ivan Krizsan
 */
@Data
@NoArgsConstructor
public abstract class Shape extends EntityWithStringId {
    /* Constant(s): */

    /* Instance variable(s): */
    /** Shape location x-coordinate. */
    @DynamoDBAttribute
    protected int x;
    /** Shape location y-coordinate. */
    @DynamoDBAttribute
    protected int y;
    /** Shape colour. */
    @DynamoDBAttribute
    protected String colour;

    /**
     * Sets the position of the shape to the supplied coordinates.
     *
     * @param inX X-coordinate of shape position.
     * @param inY Y-coordinate of shape position.
     */
    public void setPosition(final int inX, final int inY) {
        x = inX;
        y = inY;
    }
}

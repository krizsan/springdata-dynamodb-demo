package se.ivankrizsan.springdata.dynamodb.repositories;

import org.socialsignin.spring.data.dynamodb.repository.EnableScan;
import org.springframework.data.repository.CrudRepository;
import se.ivankrizsan.springdata.dynamodb.domain.Circle;

import java.util.List;

/**
 * DynamoDB repository containing {@code Circle}s.
 *
 * @author Ivan Krizsan
 * @see Circle
 */
@EnableScan
public interface CirclesRepository extends CrudRepository<Circle, String> {

    /**
     * Finds circles that which colour matches the supplied colour.
     *
     * @param colour Colour to match.
     * @return Circles which colour match.
     */
    List<Circle> findCirclesByColour(final String colour);
}

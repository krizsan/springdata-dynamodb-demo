package se.ivankrizsan.springdata.dynamodb.repositories;

import org.socialsignin.spring.data.dynamodb.repository.EnableScan;
import org.springframework.data.repository.CrudRepository;
import se.ivankrizsan.springdata.dynamodb.domain.Rectangle;

/**
 * DynamoDB repository containing {@code Rectangle}s.
 *
 * @author Ivan Krizsan
 * @see Rectangle
 */
@EnableScan
public interface RectanglesRepository extends CrudRepository<Rectangle, String> {
}

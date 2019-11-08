package model;

import java.util.Collection;

import org.springframework.data.repository.CrudRepository;

public interface KafkaRecordRepo extends CrudRepository<KafkaRecord, String> {
	
	Collection<KafkaRecord> findByKey(String key);
	Collection<KafkaRecord> findByValue(String value);
	
}

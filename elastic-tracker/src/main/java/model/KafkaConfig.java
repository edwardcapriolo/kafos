package model;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;

@Configuration
public class KafkaConfig {

	public @Autowired KafkaRecordRepo repository;
	public @Autowired ElasticsearchOperations operations;
	
}

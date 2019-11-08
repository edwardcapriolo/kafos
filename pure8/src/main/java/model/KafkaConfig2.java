package model;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

@Configuration
public class KafkaConfig2 {

	@Bean
	RestHighLevelClient client() {
		ClientConfiguration client = ClientConfiguration.builder().connectedTo("localhost:9200").build();
		return RestClients.create(client).rest();
	}
	
	@Bean
	public ElasticsearchRestTemplate elasticsearchTemplate() {
		return new ElasticsearchRestTemplate(client());
	}
	
}

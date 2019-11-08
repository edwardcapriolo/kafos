package model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "kafka_record", refreshInterval = "-1")
public class KafkaRecord {
	@Id
	public String id;
	
	@Field(type = FieldType.Text, index=true) 
	public String key;
	
	@Field(type = FieldType.Text, index=true)
	public String value;
	
	@Field(type = FieldType.Long, index=true)
	public Long offset;
	
	@Field(type = FieldType.Long, index=true)
	public Long partition;

	public KafkaRecord() {


	}

	@Override
	public String toString() {
		return "KafkaRecord [id=" + id + ", key=" + key + ", value=" + value + ", offset=" + offset + ", partition="
				+ partition + "]";
	}
	
	
}

package model;

import java.util.Collection;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner; 

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class KafkaRepoTest {

	@Autowired KafkaRecordRepo repository;
	@Autowired ElasticsearchOperations operations;

	@Before
	public void before() {
		operations.deleteIndex(KafkaRecord.class);
		operations.createIndex(KafkaRecord.class);
		operations.refresh(KafkaRecord.class);
	}
	
	@Test
	public void nothing() {
		
		KafkaRecord kr = new KafkaRecord();
		kr.key = "bla";
		kr.value = "40";
		repository.save(kr);
		Collection<KafkaRecord> l = repository.findByKey("bla");
		Assert.assertEquals(1, l.size());
		Collection<KafkaRecord> b = repository.findByValue("40");
		Assert.assertEquals(1, b.size());
	}
}

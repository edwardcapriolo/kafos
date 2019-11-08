package model;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Injector {
	
	public ClassPathXmlApplicationContext ctx;
	
	public KafkaConfig kafkaConfig;
	//public @Autowired KafkaRecordRepo repository;
	//public @Autowired ElasticsearchOperations operations;
	
	public Injector() {
	   ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
	   kafkaConfig = (KafkaConfig) ctx.getBean("kafkaConfig");
	   //ctx.getAutowireCapableBeanFactory().autowireBean(repository);
	   //ctx.getAutowireCapableBeanFactory().autowireBean(operations);
	   
	}
	
	public void close() {
		ctx.destroy();
	}

}

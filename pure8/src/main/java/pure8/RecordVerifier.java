package pure8;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import model.KafkaRecord;
import model.KafkaRecordRepo;

public class RecordVerifier {

	public RecordVerifier(int start, int end, KafkaRecordRepo repo) {
		
		long notFound = 0;
		long foundCorrect = 0;
		long foundDuplications = 0;
		List<String> notFoundList = new ArrayList<>();
		for (int i=0; i < end; i++) {
			Collection<KafkaRecord> found = repo.findByValue(String.valueOf(i));
			if (found.size() == 0) {
				notFound ++;
				notFoundList.add(i+"");
			} else if (found.size() == 1) {
				foundCorrect++;
			} else if (found.size() > 1) {
				foundDuplications++;
			} else {
				throw new RuntimeException ("bad logic");
			}
		}
		System.out.println("Verification stats");
		System.out.println("------------------------------------------------------------------");
		System.out.println("notFound: "+ notFound +" foundCorrect: "+ foundCorrect +" foundDuplications: "+ foundDuplications);
		System.out.println("------------------------------------------------------------------");
		System.out.println("not found list" + notFoundList);
		//System.out.println(repo.findAll());
	}
	
}

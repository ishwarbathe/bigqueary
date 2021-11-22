
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

// Sample to query in a table
public class Query {

  public static void main(String[] args) throws FileNotFoundException, IOException {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "newscorp-newsiq";
    String datasetName = "temporary_analysis";
    String tableName = "globalqa";
//    String query =
//        "SELECT * "
//            + " FROM `"
//            + projectId
//            + "."
//            + datasetName
//            + "."
//            + tableName;
    
    String query = "Select * from temporary_analysis.globalqa";
    query(query);
  }

  public static void query(String query) throws FileNotFoundException, IOException {
    try {
    	
    	  // TODO(developer): Replace these variables before running the sample.
	    String projectId = "newscorp-newsiq";
	    File credentialsPath = new File("C:\\Users\\dell\\Downloads\\newscorp-newsiq-217c60cd1e66 (1).json");

	    // Load credentials from JSON key file. If you can't set the GOOGLE_APPLICATION_CREDENTIALS
	    // environment variable, you can explicitly load the credentials file to construct the
	    // credentials.
	    GoogleCredentials credentials;
	    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
	      credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
	    }
	    
	    
	    // Instantiate a client.
	    BigQuery bigquery =
	        BigQueryOptions.newBuilder()
	            .setCredentials(credentials)
	            .setProjectId(projectId)
	            .build()
	            .getService();

      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

      TableResult results = bigquery.query(queryConfig);

      results
          .iterateAll()
          .forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));

      System.out.println("Query performed successfully.");
    } catch (BigQueryException | InterruptedException e) {
      System.out.println("Query not performed \n" + e.toString());
    }
  }
}
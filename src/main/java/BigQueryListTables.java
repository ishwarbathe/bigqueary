
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;

public class BigQueryListTables {

  public static void main(String[] args) throws FileNotFoundException, IOException {
    // TODO(developer): Replace these variables before running the sample.
	  String projectId = "newscorp-newsiq";
    String datasetName = "temporary_analysis";
    listTables(projectId, datasetName);
  }

  public static void listTables(String projectId, String datasetName) throws FileNotFoundException, IOException {
    try {
    	  // TODO(developer): Replace these variables before running the sample.
	    
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


      DatasetId datasetId = DatasetId.of(projectId, datasetName);
      Page<Table> tables = bigquery.listTables(datasetId, TableListOption.pageSize(100));
      tables.iterateAll().forEach(table -> System.out.print(table.getTableId().getTable() + "\n"));

      System.out.println("Tables listed successfully.");
    } catch (BigQueryException e) {
      System.out.println("Tables were not listed. Error occurred: " + e.toString());
    }
  }
}
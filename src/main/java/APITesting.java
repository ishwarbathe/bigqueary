import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class APITesting {

	// [START bigquery_client_default_credentials]
	  public static void implicit() {
	    // Instantiate a client. If you don't specify credentials when constructing a client, the
	    // client library will look for credentials in the environment, such as the
	    // GOOGLE_APPLICATION_CREDENTIALS environment variable.
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

	    // Use the client.
	    System.out.println("Datasets:");
	    for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
	    	System.out.println("----------------------------------------------");
	      System.out.printf("%s%n", dataset.getDatasetId().getDataset());
	      System.out.printf("%s%n", dataset.getDatasetId());
	    
//	      if(dataset.getDatasetId().getDataset().equals("temporary_analysis")){
//	    	
//	      }
	    }
	  }
	  // [END bigquery_client_default_credentials]

	  // [START bigquery_client_json_credentials]
	  public static void explicit() throws IOException {
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

	    // Use the client.
	    System.out.println("Datasets:");
	    for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
	    	System.out.println("***************************************************");
		      System.out.printf("%s%n", dataset.getDatasetId().getDataset());
		      System.out.printf("%s%n", dataset.getDatasetId());
	    }
	  }
	  // [END bigquery_client_json_credentials]

	  public static void main(String... args) throws IOException {
	   // boolean validArgs = args.length == 1;
	    String sample = "explicit";
//	    if (validArgs) {
//	      sample = args[0];
//	      if (!sample.equals("explicit") && !sample.equals("implicit")) {
//	        validArgs = false;
//	      }
//	    }

//	    if (!validArgs) {
//	      System.err.println("Expected auth type argument: implict|explict");
//	      System.exit(1);
//	    }

	    if (sample.equals("implicit")) {
	      implicit();
	    } else {
	      explicit();
	    }
	  }

}

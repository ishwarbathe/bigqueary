/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// [START bigquery_get_view]
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

// Sample to get a view
public class GetView {

	static String projectId = "newscorp-newsiq";

	public static void runGetView() throws FileNotFoundException, IOException {
		// TODO(developer): Replace these variables before running the sample.

		String datasetName = "temporary_analysis";
		String viewName = "globalqa";
		getView(datasetName, viewName);
	}

	public static void getView(String datasetName, String viewName) throws FileNotFoundException, IOException {
		try {
			File credentialsPath = new File("C:\\Users\\dell\\Downloads\\newscorp-newsiq-217c60cd1e66 (1).json");

			// Load credentials from JSON key file. If you can't set the
			// GOOGLE_APPLICATION_CREDENTIALS
			// environment variable, you can explicitly load the credentials
			// file to construct the
			// credentials.
			GoogleCredentials credentials;
			try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
				credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
			}

			// Instantiate a client.
			BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build()
					.getService();

			TableId tableId = TableId.of(datasetName, viewName);
			Table view = bigquery.getTable(tableId);
			System.out.println("View retrieved successfully" + view.getDescription());
		} catch (BigQueryException e) {
			System.out.println("View not retrieved. \n" + e.toString());
		}
	}
	
	public static void main(String...strings) throws FileNotFoundException, IOException{
		runGetView();
	}
}
// [END bigquery_get_view]
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

// [START bigquery_grant_view_access]
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Sample to grant view access on dataset
public class GrantViewAccess {
	
	static String projectId = "newscorp-newsiq";

  public static void runGrantViewAccess() throws IOException {
    // TODO(developer): Replace these variables before running the sample.
    String srcDatasetId = "temporary_analysis";
    String viewDatasetId = "globalqa";
    String viewId = "globalqa";
    grantViewAccess(srcDatasetId, viewDatasetId, viewId);
  }

  public static void grantViewAccess(String srcDatasetId, String viewDatasetId, String viewId) throws IOException {
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

      Dataset srcDataset = bigquery.getDataset(DatasetId.of(srcDatasetId));
      Dataset viewDataset = bigquery.getDataset(DatasetId.of(viewDatasetId));
      Table view = viewDataset.get(viewId);

      // First, we'll add a group to the ACL for the dataset containing the view. This will allow
      // users within that group to query the view, but they must have direct access to any tables
      // referenced by the view.
      List<Acl> viewAcl = new ArrayList<>();
      viewAcl.addAll(viewDataset.getAcl());
      viewAcl.add(Acl.of(new Acl.Group("globalqa@newscorp-newsiq.iam.gserviceaccount.com"), Acl.Role.OWNER));
      viewDataset.toBuilder().setAcl(viewAcl).build().update();

      // Now, we'll authorize a specific view against a source dataset, delegating access
      // enforcement. Once this has been completed, members of the group previously added to the
      // view dataset's ACL no longer require access to the source dataset to successfully query the
      // view
      List<Acl> srcAcl = new ArrayList<>();
      srcAcl.addAll(srcDataset.getAcl());
      srcAcl.add(Acl.of(new Acl.View(view.getTableId())));
      srcDataset.toBuilder().setAcl(srcAcl).build().update();
      System.out.println("Grant view access successfully");
    } catch (BigQueryException e) {
      System.out.println("Grant view access was not success. \n" + e.toString());
    }
  }
  
  public static void main(String...strings) throws FileNotFoundException, IOException{
	  runGrantViewAccess();
	}
}
// [END bigquery_grant_view_access]
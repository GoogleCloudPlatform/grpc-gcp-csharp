using System.Diagnostics;
using Google.Cloud.Firestore.V1Beta1;

namespace FirestoreProbesTest
{

	public class FirestoreProbesTestClass
	{
		private readonly Stopwatch stopwatch;
		private static String _PARENT_RESOURCE = "projects/cloudprober-testing/databases/(default)/documents";
		public static Dictionary<string, string> probFunctions;

		public FirestoreProbesTestClass()
		{
			//Constructor
			this.stopwatch = new Stopwatch();
			this.probFunctions = new Dictionary<string, string>(){
				{"documents", "document"}
			};
		}

		public static void document(Client client, ref Dictionary<string, long> metrics){

			ListDocumentsRequest list_document_request = new ListDocumentsRequest();
			list_document_request.setParent(_PARENT_RESOURCE);

			stopwatch.Start();
			client.ListDocuments(list_document_request);
			stopwatch.Stop();

			long lantency = stopwatch.ElapsedMilliseconds;
			metrics.Add("list_documents_latency_ms", lantency);
		}
	}
}



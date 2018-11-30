using FirestoreProbesTest;
using Google.Cloud.Firestore.V1beta1;
using Google.Cloud.Spanner.V1;
using Google.Apis.Auth.OAuth2;
using System;
using System.Reflection;
using StackdriverUtil;

namespace ProberTest
{
	public class Prober
	{
		private static string _OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
		private static string _FIRESTORE_TARGET = "firestore.googleapis.com:443";
		private static string _SPANNER_TARGET = "spanner.googleapis.com:443";

		static int Main(string[] args)
		{
			if (args.Length == 0){
				Console.WriteLine("Please enter a numeric argument");
				return 1;
			}

			executeProbes(args[1]);
			return 0;
		}

		public static void executeProbes(string api)
		{
			var util = new StackdriverUtil(api);
			var auth = ApplicationDefaultCredentials.getCredentials(_OAUTH_SCOPE);

			if (api == "firesotre")
			{
				var client = new FirestoreClient(_FIRESTORE_TARGET, opts);
				var test = new FirestoreProbesTestClass();
				var probe_functions = test.probFunctions;
			}
			else if(api == "spanner")
			{
				var client = new SpannerClient(_SPANNER_TARGET, opts);
				var test = new SpannerProbesTestClass();
				var probe_functions = test.probeFunctions;
			}
			else
			{
				Console.WriteLine("grpc not implemented for {0}", api);
				return;
			}

			int total = probe_functions.Count;
			int success = 0;
			Dictionary<string, long> metrics = new Dictionary<string, long>();

			foreach (var probe in probe_functions){
				Console.WriteLine("{0}", probe.Key);
				MethodInfo fun = test.GetType().GetMethod(probe.Value);
				fun.Invoke(test, [client, metrics]);
				success ++;
			}

			if(success == total){
				// TODO
				util.setSuccess(true);
			}
			util.addMetrics(metrics);
			util.outputMetrics();

			if(success != total){
				return;
			}
		}
	}
}
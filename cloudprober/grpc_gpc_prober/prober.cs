using FirestoreProbesTest;
using SpannerProbesTest;
using ProbeTestsBase;
using Google.Cloud.Firestore.V1Beta1;
using Google.Cloud.Spanner.V1;
using Google.Apis.Auth.OAuth2;
using Grpc.Core;
using Grpc.Gcp;
using Grpc.Auth;
using System;
using System.Reflection;
using StackdriverUtil;
using System.Collections.Generic;

namespace ProberTest
{
	public class Prober
	{
		private static string _OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
		private static string _FIRESTORE_TARGET = "firestore.googleapis.com:443";
		private static string _SPANNER_TARGET = "spanner.googleapis.com:443";

        private static ApiConfig config = new ApiConfig();
        private static GcpCallInvoker invoker;

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
            StackdriverUtilClass util = new StackdriverUtilClass(api);
            GoogleCredential auth = GoogleCredential.GetApplicationDefault();
            IList<ChannelOption> opts = new List<ChannelOption>(){
                new ChannelOption(GcpCallInvoker.ApiConfigChannelArg, config.ToString()) };
            ClientBase client;
            ProbeTestsBaseClass test;

            Dictionary<string, string> probe_functions = new Dictionary<string, string>();

			if (api == "firesotre")
			{
                invoker = new GcpCallInvoker(_FIRESTORE_TARGET, auth.ToChannelCredentials(), opts);
                client = new Firestore.FirestoreClient(invoker);
				test = new FirestoreProbesTestClass();
			}
			else if(api == "spanner")
			{
                invoker = new GcpCallInvoker(_SPANNER_TARGET, auth.ToChannelCredentials(), opts);
				client = new Spanner.SpannerClient(invoker);
				test = new SpannerProbesTestClass();
			}
			else
			{
				Console.WriteLine("grpc not implemented for {0}", api);
				return;
			}

            //object value = test.GetType().GetMethod("GetProbFunctions").Invoke(test,null);
            //probe_functions = value.GetType().GetProperties();

			int total = probe_functions.Count;
			int success = 0;
			Dictionary<string, long> metrics = new Dictionary<string, long>();

			foreach (var probe in probe_functions){
				Console.WriteLine("{0}", probe.Key);
				MethodInfo fun = test.GetType().GetMethod(probe.Value);
                object[] parameters = new object[] { client, metrics };
				fun.Invoke(test, parameters);
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


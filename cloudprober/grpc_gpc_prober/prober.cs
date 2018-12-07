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
		//private static string _OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
		private static string _FIRESTORE_TARGET = "firestore.googleapis.com:443";
		private static string _SPANNER_TARGET = "spanner.googleapis.com:443";

        private static ApiConfig config = new ApiConfig();

        static int Main(string[] args)
		{
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "../../../../cloudprober-test-312fec66d8c5.json");
            if (args.Length == 0){
				Console.WriteLine("Please enter a numeric argument");
				return 1;
			}

            if (args[0] != "--api")
            {
                Console.WriteLine("Type (--api api_name) to continue ...");
                return 1;
            }

            executeProbes(args[1]);
			return 0;
		}

        public static void executeProbes(string api)
        {
            StackdriverUtilClass util = new StackdriverUtilClass(api);
            GoogleCredential auth = GoogleCredential.GetApplicationDefault();
            //GoogleCredential auth = GoogleCredential.FromFile(Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS"));
            ClientBase client;
            ProbeTestsBaseClass test;
            System.Type type;

            Dictionary<string, string> probe_functions = new Dictionary<string, string>();

			if (api == "firestore")
			{
                Grpc.Core.Channel channel = new Grpc.Core.Channel(_FIRESTORE_TARGET, GoogleGrpcCredentials.ToChannelCredentials(auth));
                client = new Firestore.FirestoreClient(channel);
				test = new FirestoreProbesTestClass();
                probe_functions = (test as FirestoreProbesTestClass).GetProbFunctions();
                type = typeof(FirestoreProbesTestClass);
            }
			else if(api == "spanner")
			{
                Grpc.Core.Channel channel = new Grpc.Core.Channel(_SPANNER_TARGET, GoogleGrpcCredentials.ToChannelCredentials(auth));
                client = new Spanner.SpannerClient(channel);
				test = new SpannerProbesTestClass();
                probe_functions = (test as SpannerProbesTestClass).GetProbFunctions();
                type = typeof(SpannerProbesTestClass);
            }
			else
			{
				Console.WriteLine("grpc not implemented for {0}", api);
				return;
			}

            //object value = test.GetType().GetMethod("GetProbFunctions").Invoke(test,null);
            //probe_functions = value.GetType().GetProperties()

			int total = probe_functions.Count;
			int success = 0;
			Dictionary<string, long> metrics = new Dictionary<string, long>();

			foreach (var probe in probe_functions){
				Console.WriteLine("{0}", probe.Key);
				MethodInfo fun = type.GetMethod(probe.Value);
                object[] parameters = new object[] { client, metrics };

                try
                {
                    if(api == "firestore")
                    {
                        fun.Invoke((test as FirestoreProbesTestClass), parameters);
                    }
                    else
                    {
                        fun.Invoke((test as SpannerProbesTestClass), parameters);
                    }
                
                    success++;
                }
                catch(Exception error)
                {
                    Console.WriteLine("{0}", error);
                    util.reportError(error);
                }
            }

			if(success == total){
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


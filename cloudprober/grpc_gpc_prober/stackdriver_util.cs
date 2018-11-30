using System;
using Google.Apis.Clouderrorreporting.v1beta1;
using Google.Cloud.ErrorReporting.V1Beta1;

namespace StackdriverUtil
{
	public class StackdriverUtil
	{
		private string api;
		private Dictionary<string, string> metrics;
		private bool success;
		private ReportErrorServiceClient err_client;

		public StackdriverUtil(string api)
		{
			this.api = api;
			this.metrics = new Dictionary<string, string>;
			this.success = false;
			this.err_client =  new ReportErrorServiceClient.Create();
		}

		public void addMetric(string key, string val)
		{
			this.metrics.Add(key, val);
			return
		}

		public void addMetrics(Dictionary<string, string> metrics)
		{
			metrics.ToList().Foreach(x => this.metrics.Add(x.Key, x.Value));
			return
		}

		public void setSuccess(bool result)
		{
			this.success = result;
			return
		}

		public void outputMetrics()
		{
			if (this.success)
			{
				Console.WriteLine("_success 1");
			}
			else
			{
				Console.WriteLine("_success 0");
			}
			foreach(var ele in this.metrics)
			{
				Console.WriteLine("{0} {1}",ele.Key, ele.Value);
			}
		}

		public void reportError(var err)
		{
			// TODO
			string projectId = "434076015357";
			string project_name = this.err_client.ProjectName();

			ReportedErrorEvent error_event = new ReportedErrorEvent();
			error_event.setMessage("CSharpProbeFailure: fails on {0} API. Details: {1}", this.api, err.ToString());

			this.err_client.ReportErrorEvent(project_name, error_event);
		}
	}
}
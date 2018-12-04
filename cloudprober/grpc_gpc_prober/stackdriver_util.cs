using System;
using System.Collections.Generic;
using System.Linq;
using Google.Cloud.ErrorReporting.V1Beta1;

namespace StackdriverUtil
{
	public class StackdriverUtilClass
	{
		private string api;
		private Dictionary<string, long> metrics;
		private bool success;
		private ErrorStatsService.ErrorStatsServiceClient err_client;

		public StackdriverUtilClass(string api)
		{
			this.api = api;
			this.metrics = new Dictionary<string, long>();
			this.success = false;
			this.err_client =  new ErrorStatsService.ErrorStatsServiceClient();
		}

		public void addMetric(string key, long val)
		{
			this.metrics.Add(key, val);
			return;
		}

		public void addMetrics(Dictionary<string, long> metrics)
		{
			metrics.ToList().ForEach(x => this.metrics.Add(x.Key, x.Value));
			return;
		}

		public void setSuccess(bool result)
		{
			this.success = result;
			return;
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
			return;
		}

		public void reportError(var err)
		{
			// TODO
			string projectId = "434076015357";
			string project_name = this.err_client.ProjectName(projectId);

			ReportedErrorEvent error_event = new ReportedErrorEvent();
			error_event.setMessage("CSharpProbeFailure: fails on {0} API. Details: {1}", this.api, err.ToString());

			this.err_client.ReportErrorEvent(project_name, error_event);
			return;
		}
	}
}
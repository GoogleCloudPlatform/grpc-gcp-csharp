using System;
using System.Collections.Generic;
using System.Linq;
using Google.Cloud.ErrorReporting.V1Beta1;
using Google.Api.Gax.ResourceNames;

namespace StackdriverUtil
{
	public class StackdriverUtilClass
	{
		private string api;
		private Dictionary<string, long> metrics;
		private bool success;
		private ReportErrorsServiceClient err_client;

		public StackdriverUtilClass(string api)
		{
			this.api = api;
			this.metrics = new Dictionary<string, long>();
			this.success = false;
			this.err_client =  ReportErrorsServiceClient.Create();
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
				Console.WriteLine("{0}_success 1", this.api);
			}
			else
			{
				Console.WriteLine("{0}_success 0", this.api);
			}
			foreach(var ele in this.metrics)
			{
				Console.WriteLine("{0} {1}",ele.Key, ele.Value);
			}
			return;
		}

        public void reportError(Exception err)
        {
            string projectId = "434076015357";
            ProjectName project_name = new ProjectName(projectId);
            string err_mssage = err.ToString();

            ReportedErrorEvent error_event = new ReportedErrorEvent();
            ErrorContext context = new ErrorContext();
            SourceLocation location = new SourceLocation();
            location.FunctionName = this.api;
            context.ReportLocation = location;
            error_event.Context = context;
            error_event.Message = "CSharpProbeFailure: fails on {$this.api} API. Details: {$err_message}";

            this.err_client.ReportErrorEvent(project_name, error_event);
			return;
		}
	}
}
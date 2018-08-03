using Microsoft.Extensions.CommandLineUtils;
using System;

namespace Grpc.Gcp.Benchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            CommandLineApplication app =
                new CommandLineApplication(throwOnUnexpectedArg: false);
            CommandOption numStreamCallsOption = app.Option(
                "-s |--num_stream_calls <NUM_STREAM_CALLS>",
                "The number of active streams to establish.",
                CommandOptionType.SingleValue);
            CommandOption gcpOption = app.Option(
                "-g | --gcp", "Use Grpc.Gcp call invoker feature.",
                CommandOptionType.NoValue);
            app.OnExecute(() =>
            {
                int numStreamCalls = 1;
                if (numStreamCallsOption.HasValue()) {
                    numStreamCalls = Int32.Parse(numStreamCallsOption.Value());
                }
                BigtableBenchmark benchmark = new BigtableBenchmark(numStreamCalls, gcpOption.HasValue());
                benchmark.RunMaxConcurrentStreams();
                return 0;
            });
            app.Execute(args);
        }
    }
}
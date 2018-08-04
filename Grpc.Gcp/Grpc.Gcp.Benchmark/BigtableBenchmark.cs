using System;
using System.Collections.Generic;
using System.Threading;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using Grpc.Auth;
using Grpc.Core;
using Microsoft.Extensions.CommandLineUtils;

namespace Grpc.Gcp.Benchmark
{
    class BigtableBenchmark
    {
        private const string Target = "bigtable.googleapis.com";
        private const string TableName = "projects/grpc-gcp/instances/test-instance/tables/test-table";
        private const string RowKey = "test-row";
        private const string TestValue = "test-value";
        private const string ColumnFamily = "test-cf";
        private const string ColumnQualifier = "test-cq";
        private const string LargeRowKey = "large-row";
        private const Int32 PayloadBytes = 10000000;
        private const Int32 DefaultMaxChannelsPerTarget = 10;
        private ApiConfig config = new ApiConfig();
        private Bigtable.BigtableClient client;
        private int numStreamCalls;
        private bool useGcp;

        public BigtableBenchmark(int numStreamCalls, bool gcp) {
            this.numStreamCalls = numStreamCalls;
            this.useGcp = gcp;
            if (gcp) {
                InitGcpClient();
            } else {
                InitDefaultClient();
            }
        }

        private void InitGcpClient()
        {
            InitApiConfig(100, 10);
            GoogleCredential credential = GoogleCredential.GetApplicationDefault();
            IList<ChannelOption> options = new List<ChannelOption>() {
                new ChannelOption(GcpCallInvoker.API_CONFIG_CHANNEL_ARG, config.ToString()) };
            var invoker = new GcpCallInvoker(Target, credential.ToChannelCredentials(), options);
            client = new Bigtable.BigtableClient(invoker);
        }

        private void InitDefaultClient()
        {
            GoogleCredential credential = GoogleCredential.GetApplicationDefault();
            var channel = new Channel(Target, credential.ToChannelCredentials());
            client = new Bigtable.BigtableClient(channel);
        }

        private void InitApiConfig(uint maxConcurrentStreams, uint maxSize)
        {
            config.ChannelPool = new ChannelPoolConfig();
            config.ChannelPool.MaxConcurrentStreamsLowWatermark = maxConcurrentStreams;
            config.ChannelPool.MaxSize = maxSize;
        }

        private void PrepareTestData()
        {
            MutateRowRequest mutateRowRequest = new MutateRowRequest
            {
                TableName = TableName,
                RowKey = ByteString.CopyFromUtf8(LargeRowKey)
            };

            string largeValue = new string('x', PayloadBytes);

            Mutation mutation = new Mutation
            {
                SetCell = new Mutation.Types.SetCell
                {
                    FamilyName = ColumnFamily,
                    ColumnQualifier = ByteString.CopyFromUtf8(ColumnQualifier),
                    Value = ByteString.CopyFromUtf8(largeValue),
                }
            };

            mutateRowRequest.Mutations.Add(mutation);
            client.MutateRow(mutateRowRequest);
        }

        public void RunMaxConcurrentStreams()
        {
            PrepareTestData();

            var calls = new List<AsyncServerStreamingCall<ReadRowsResponse>>();

            for (int i = 0; i < numStreamCalls; i++)
            {
                var streamingCall = client.ReadRows(
                    new ReadRowsRequest
                    {
                        TableName = TableName,
                        Rows = new RowSet
                        {
                            RowKeys = { ByteString.CopyFromUtf8("large-row") }
                        }
                    });
                calls.Add(streamingCall);
            }
            Console.WriteLine(String.Format("Created {0} streaming calls.", numStreamCalls));

            CancellationTokenSource tokenSource = new CancellationTokenSource();
            CancellationToken token = tokenSource.Token;

            Console.WriteLine("Starting UnaryUnary blocking call..");
            var watch = System.Diagnostics.Stopwatch.StartNew();
            MutateRowRequest mutateRowRequest = new MutateRowRequest
            {
                TableName = TableName,
                RowKey = ByteString.CopyFromUtf8(RowKey)
            };

            Mutation mutation = new Mutation
            {
                SetCell = new Mutation.Types.SetCell
                {
                    FamilyName = ColumnFamily,
                    ColumnQualifier = ByteString.CopyFromUtf8(ColumnQualifier),
                    Value = ByteString.CopyFromUtf8(TestValue),
                }
            };

            mutateRowRequest.Mutations.Add(mutation);

            // Set 5 sec time out for the blocking call.
            client.MutateRow(mutateRowRequest, null, DateTime.UtcNow.AddSeconds(5));

            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            Console.WriteLine("Elapsed time for another call (ms): " + elapsedMs);

        }
    }
}

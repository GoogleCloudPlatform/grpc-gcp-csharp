using Google.Apis.Auth.OAuth2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using Grpc.Auth;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Grpc.Gcp.IntegrationTest
{
    [TestClass]
    public class BigtableTest
    {
        private const string Target = "bigtable.googleapis.com";
        private const string TableName = "projects/grpc-gcp/instances/test-instance/tables/test-table";
        private const string RowKey = "test-row";
        private const string TestValue = "test-value";
        private const string ColumnFamily = "test-cf";
        private const string ColumnQualifier = "test-cq";
        private const Int32 DefaultMaxChannelsPerTarget = 10;
        private ApiConfig config;
        private GcpCallInvoker invoker;
        private Bigtable.BigtableClient client;

        [TestInitialize]
        public void SetUp()
        {
            InitApiConfig(1, 10);
            InitClient();
        }

        private void InitClient()
        {
            GoogleCredential credential = GoogleCredential.GetApplicationDefault();
            IList<ChannelOption> options = new List<ChannelOption>() {
                new ChannelOption(GcpCallInvoker.ApiConfigChannelArg, config.ToString()) };
            invoker = new GcpCallInvoker(Target, credential.ToChannelCredentials(), options);
            client = new Bigtable.BigtableClient(invoker);
        }

        private void InitApiConfig(uint maxConcurrentStreams, uint maxSize)
        {
            config = new ApiConfig
            {
                ChannelPool = new ChannelPoolConfig
                {
                    MaxConcurrentStreamsLowWatermark = maxConcurrentStreams,
                    MaxSize = maxSize,
                }
            };
        }

        [TestMethod]
        public void MutateRow()
        {
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

            client.MutateRow(mutateRowRequest);
            Assert.AreEqual(1, invoker.channelRefs.Count);
        }

        [TestMethod]
        public void MutateRowAsync()
        {
            MutateRowRequest mutateRowRequest = new MutateRowRequest
            {
                TableName = TableName,
                RowKey = ByteString.CopyFromUtf8(RowKey),
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

            AsyncUnaryCall<MutateRowResponse> call = client.MutateRowAsync(mutateRowRequest);
            Assert.AreEqual(1, invoker.channelRefs.Count);
            Assert.AreEqual(1, invoker.channelRefs[0].ActiveStreamCount);
            MutateRowResponse response = call.ResponseAsync.Result;
            Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamCount);
        }

        [TestMethod]
        public void ReadRows()
        {
            ReadRowsRequest readRowsRequest = new ReadRowsRequest
            {
                TableName = TableName,
                Rows = new RowSet
                {
                    RowKeys = { ByteString.CopyFromUtf8(RowKey) }
                }
            };
            var streamingCall = client.ReadRows(readRowsRequest);
            Assert.AreEqual(1, invoker.channelRefs.Count);
            Assert.AreEqual(1, invoker.channelRefs[0].ActiveStreamCount);
            Assert.ThrowsException<System.InvalidOperationException>(() => streamingCall.GetStatus());

            CancellationTokenSource tokenSource = new CancellationTokenSource();
            CancellationToken token = tokenSource.Token;
            var responseStream = streamingCall.ResponseStream;
            ReadRowsResponse firstResponse = null;
            while (responseStream.MoveNext(token).Result)
            {
                if (firstResponse == null) firstResponse = responseStream.Current;
            }
            Assert.AreEqual("test-value", firstResponse.Chunks[0].Value.ToStringUtf8());
            Assert.AreEqual(1, invoker.channelRefs.Count);
            Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamCount);
            Assert.AreEqual(StatusCode.OK, streamingCall.GetStatus().StatusCode);
        }

        [TestMethod]
        public void ConcurrentStreams()
        {
            config = new ApiConfig();
            int lowWatermark = 5;
            InitApiConfig((uint)lowWatermark, 10);
            InitClient();

            var calls = new List<AsyncServerStreamingCall<ReadRowsResponse>>();

            for (int i = 0; i < lowWatermark; i++)
            {
                var streamingCall = client.ReadRows(
                    new ReadRowsRequest
                    {
                        TableName = TableName,
                        Rows = new RowSet
                        {
                            RowKeys = { ByteString.CopyFromUtf8(RowKey) }
                        }
                    });
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(i + 1, invoker.channelRefs[0].ActiveStreamCount);
                calls.Add(streamingCall);
            }

            // When number of active streams reaches the lowWaterMark,
            // New channel should be created.
            var anotherStreamingCall = client.ReadRows(
                new ReadRowsRequest
                {
                    TableName = TableName,
                    Rows = new RowSet
                    {
                        RowKeys = { ByteString.CopyFromUtf8(RowKey) }
                    }
                });
            Assert.AreEqual(2, invoker.channelRefs.Count);
            Assert.AreEqual(lowWatermark, invoker.channelRefs[0].ActiveStreamCount);
            Assert.AreEqual(1, invoker.channelRefs[1].ActiveStreamCount);
            calls.Add(anotherStreamingCall);

            // Clean open streams.
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            CancellationToken token = tokenSource.Token;
            for (int i = 0; i < calls.Count; i++)
            {
                var responseStream = calls[i].ResponseStream;
                while (responseStream.MoveNext(token).Result) { };
            }
            Assert.AreEqual(2, invoker.channelRefs.Count);
            Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamCount);
            Assert.AreEqual(0, invoker.channelRefs[1].ActiveStreamCount);
        }

        [TestMethod]
        public void AsyncCallsWithNewChannels()
        {
            var calls = new List<AsyncServerStreamingCall<ReadRowsResponse>>();

            for (int i = 0; i < DefaultMaxChannelsPerTarget; i++)
            {
                var streamingCall = client.ReadRows(
                    new ReadRowsRequest
                    {
                        TableName = TableName,
                        Rows = new RowSet
                        {
                            RowKeys = { ByteString.CopyFromUtf8(RowKey) }
                        }
                    });
                Assert.AreEqual(i + 1, invoker.channelRefs.Count);
                calls.Add(streamingCall);
            }

            // When number of channels reaches the max, old channels will be reused,
            // even when the number of active streams is higher than the watermark.
            for (int i = 0; i < DefaultMaxChannelsPerTarget; i++)
            {
                var streamingCall = client.ReadRows(
                    new ReadRowsRequest
                    {
                        TableName = TableName,
                        Rows = new RowSet
                        {
                            RowKeys = { ByteString.CopyFromUtf8(RowKey) }
                        }
                    });
                Assert.AreEqual(DefaultMaxChannelsPerTarget, invoker.channelRefs.Count);
                calls.Add(streamingCall);
            }

            // Clean open streams.
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            CancellationToken token = tokenSource.Token;
            for (int i = 0; i < calls.Count; i++)
            {
                var responseStream = calls[i].ResponseStream;
                while (responseStream.MoveNext(token).Result) { };
            }
            Assert.AreEqual(DefaultMaxChannelsPerTarget, invoker.channelRefs.Count);

            var channelRefs = invoker.channelRefs;
            for (int i = 0; i < channelRefs.Count; i++)
            {
                var channel = channelRefs[i].Channel;
                var state = channel.State;
                Assert.AreEqual(ChannelState.Ready, channel.State);
            }

            // Shutdown all channels in the channel pool.
            invoker.ShutdownAsync().Wait();

            for (int i = 0; i < channelRefs.Count; i++)
            {
                var channel = channelRefs[i].Channel;
                Assert.AreEqual(ChannelState.Shutdown, channel.State);
            }
        }

        [TestMethod]
        public void CreateClientWithEmptyOptions()
        {
            GoogleCredential credential = GoogleCredential.GetApplicationDefault();
            invoker = new GcpCallInvoker(Target, credential.ToChannelCredentials());
            client = new Bigtable.BigtableClient(invoker);

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

            client.MutateRow(mutateRowRequest);
            Assert.AreEqual(1, invoker.channelRefs.Count);
        }
    }
}

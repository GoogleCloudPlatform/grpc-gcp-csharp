using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Google.Cloud.Bigtable.V2;
using Google.Apis.Auth.OAuth2;
using System.IO;
using Grpc.Core;
using System.Collections.Generic;
using Google.Protobuf;
using System.Text;
using Grpc.Auth;
using System.Threading;

namespace Grpc.Gcp.IntegrationTest
{
    [TestClass]
    public class BigtableTest
    {
        private const string TARGET = "bigtable.googleapis.com";
        private const string TABLE = "projects/grpc-gcp/instances/test-instance/tables/test-table";
        private const string OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
        private const string ROW_KEY = "test-row";
        private const string VALUE = "test-value";
        private const string COLUMN_FAMILY = "test-cf";
        private const string COLUMN_QUALIFIER = "test-cq";
        private const Int32 DEFAULT_MAX_CHANNELS_PER_TARGET = 10;
        private ApiConfig config = new ApiConfig();
        private DefaultCallInvoker invoker;
        private Bigtable.BigtableClient client;

        [TestInitialize]
        public void SetUp()
        {
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "C:\\Users\\weiranf\\keys\\grpc-gcp-7ed990546b68.json");
            InitApiConfig(1, 10);
            InitClient();
        }

        private void InitClient()
        {
            GoogleCredential credential = GoogleCredential.GetApplicationDefault();
            IList<ChannelOption> options = new List<ChannelOption>() {
                new ChannelOption(DefaultCallInvoker.API_CONFIG_CHANNEL_ARG, config.ToString()) };
            invoker = new DefaultCallInvoker(TARGET, credential.ToChannelCredentials(), options);
            client = new Bigtable.BigtableClient(invoker);
        }

        private void InitApiConfig(uint maxConcurrentStreams, uint maxSize)
        {
            config.ChannelPool = new ChannelPoolConfig();
            config.ChannelPool.MaxConcurrentStreamsLowWatermark = maxConcurrentStreams;
            config.ChannelPool.MaxSize = maxSize;
        }

        private void AddMethod(ApiConfig config, string name, AffinityConfig.Types.Command command, string affinityKey)
        {
            MethodConfig method = new MethodConfig();
            method.Name.Add(name);
            method.Affinity = new AffinityConfig();
            method.Affinity.Command = command;
            method.Affinity.AffinityKey = affinityKey;
            config.Method.Add(method);
        }

        [TestMethod]
        public void MutateRow()
        {
            MutateRowRequest mutateRowRequest = new MutateRowRequest
            {
                TableName = TABLE,
                RowKey = ByteString.CopyFromUtf8(ROW_KEY)
            };

            Mutation mutation = new Mutation
            {
                SetCell = new Mutation.Types.SetCell
                {
                    FamilyName = COLUMN_FAMILY,
                    ColumnQualifier = ByteString.CopyFromUtf8(COLUMN_QUALIFIER),
                    Value = ByteString.CopyFromUtf8(VALUE),
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
                TableName = TABLE,
                RowKey = ByteString.CopyFromUtf8(ROW_KEY),
            };

            Mutation mutation = new Mutation
            {
                SetCell = new Mutation.Types.SetCell
                {
                    FamilyName = COLUMN_FAMILY,
                    ColumnQualifier = ByteString.CopyFromUtf8(COLUMN_QUALIFIER),
                    Value = ByteString.CopyFromUtf8(VALUE),
                }
            };

            mutateRowRequest.Mutations.Add(mutation);

            AsyncUnaryCall<MutateRowResponse> call = client.MutateRowAsync(mutateRowRequest);
            Assert.AreEqual(1, invoker.channelRefs.Count);
            Assert.AreEqual(1, invoker.channelRefs[0].ActiveStreamRef);
            MutateRowResponse response = call.ResponseAsync.Result;
            Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
        }

        [TestMethod]
        public void ReadRows()
        {
            ReadRowsRequest readRowsRequest = new ReadRowsRequest
            {
                TableName = TABLE,
                Rows = new RowSet
                {
                    RowKeys = { ByteString.CopyFromUtf8(ROW_KEY) }
                }
            };
            var streamingCall = client.ReadRows(readRowsRequest);
            Assert.AreEqual(1, invoker.channelRefs.Count);
            Assert.AreEqual(1, invoker.channelRefs[0].ActiveStreamRef);

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
            Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
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
                        TableName = TABLE,
                        Rows = new RowSet
                        {
                            RowKeys = { ByteString.CopyFromUtf8(ROW_KEY) }
                        }
                    });
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(i + 1, invoker.channelRefs[0].ActiveStreamRef);
                calls.Add(streamingCall);
            }

            // When number of active streams reaches the lowWaterMark,
            // New channel should be created.
            var anotherStreamingCall = client.ReadRows(
                new ReadRowsRequest
                {
                    TableName = TABLE,
                    Rows = new RowSet
                    {
                        RowKeys = { ByteString.CopyFromUtf8(ROW_KEY) }
                    }
                });
            Assert.AreEqual(2, invoker.channelRefs.Count);
            Assert.AreEqual(lowWatermark, invoker.channelRefs[0].ActiveStreamRef);
            Assert.AreEqual(1, invoker.channelRefs[1].ActiveStreamRef);
            calls.Add(anotherStreamingCall);

            // Clean open streams.
            // Clean open streams.
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            CancellationToken token = tokenSource.Token;
            for (int i = 0; i < calls.Count; i++)
            {
                var responseStream = calls[i].ResponseStream;
                while (responseStream.MoveNext(token).Result) { };
            }
            Assert.AreEqual(2, invoker.channelRefs.Count);
            Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
            Assert.AreEqual(0, invoker.channelRefs[1].ActiveStreamRef);
        }

    }
}

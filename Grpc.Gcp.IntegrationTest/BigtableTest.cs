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
            InitApiConfig();
            InitClient();
        }

        private void InitClient()
        {
            GoogleCredential credential = GoogleCredential.GetApplicationDefault();
            MemoryStream stream = new MemoryStream();
            config.WriteTo(stream);
            IList<ChannelOption> options = new List<ChannelOption>() {
                new ChannelOption(DefaultCallInvoker.API_CONFIG_CHANNEL_ARG, Encoding.Default.GetString(stream.ToArray())) };
            invoker = new DefaultCallInvoker(TARGET, credential.ToChannelCredentials(), options);
            client = new Bigtable.BigtableClient(invoker);
        }

        private void InitApiConfig()
        {
            config.ChannelPool = new ChannelPoolConfig();
            config.ChannelPool.MaxConcurrentStreamsLowWatermark = 1;
            config.ChannelPool.MaxSize = 10;
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
            MutateRowRequest mutateRowRequest = new MutateRowRequest();
            mutateRowRequest.TableName = TABLE;
            mutateRowRequest.RowKey = ByteString.CopyFromUtf8(ROW_KEY);
            Mutation.Types.SetCell setSell = new Mutation.Types.SetCell();

            setSell.FamilyName = COLUMN_FAMILY;
            setSell.ColumnQualifier = ByteString.CopyFromUtf8(COLUMN_QUALIFIER);
            setSell.Value = ByteString.CopyFromUtf8(VALUE);

            Mutation mutation = new Mutation();
            mutation.SetCell = setSell;
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

    }
}

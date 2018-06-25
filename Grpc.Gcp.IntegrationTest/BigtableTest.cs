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

namespace Grpc.Gcp.IntegrationTest
{
    [TestClass]
    public class BigtableTest
    {
        private const string TARGET = "bigtable.googleapis.com";
        private const string TABLE = "projects/grpc-gcp/instances/test-instance/tables/test-table";
        private const string OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
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
                new ChannelOption(DefaultCallInvoker.GRPC_GCP_CHANNEL_ARG_API_CONFIG, Encoding.Default.GetString(stream.ToArray())) };
            invoker = new DefaultCallInvoker(TARGET, credential.ToChannelCredentials(), options);
            client = new Bigtable.BigtableClient(invoker);
        }

        private void InitApiConfig()
        {
            config.ChannelPool = new ChannelPoolConfig();
            config.ChannelPool.MaxConcurrentStreamsLowWatermark = 1;
            config.ChannelPool.MaxSize = 10;
            //AddMethod(config, "/google.bigtable.v2.Bigtable/MutateRow", AffinityConfig.Types.Command.Bind, "name");
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
        public void TestMutateRow()
        {
            MutateRowRequest mutateRowRequest = new MutateRowRequest();
            mutateRowRequest.TableName = TABLE;
            mutateRowRequest.RowKey = ByteString.CopyFromUtf8("r1");
            Mutation.Types.SetCell setSell = new Mutation.Types.SetCell();

            setSell.FamilyName = "cf1";
            setSell.ColumnQualifier = ByteString.CopyFromUtf8("c1");
            setSell.Value = ByteString.CopyFromUtf8("test-value");

            Mutation mutation = new Mutation();
            mutation.SetCell = setSell;
            mutateRowRequest.Mutations.Add(mutation);

            client.MutateRow(mutateRowRequest);
            Assert.AreEqual(1, invoker.channelRefs.Count);
        }
    }
}

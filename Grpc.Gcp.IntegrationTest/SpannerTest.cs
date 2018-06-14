using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Google.Apis.Auth.OAuth2;
using Grpc.Auth;
using Google.Cloud.Spanner.V1;
using System.Collections.Generic;
using Grpc.Core;
using Google.Protobuf;
using System.IO;
using System.Text;

namespace Grpc.Gcp.IntegrationTest
{
    [TestClass]
    public class SpannerTest
    {
        private const string TARGET = "spanner.googleapis.com";
        private const string DATABASE = "projects/grpc-gcp/instances/sample/databases/benchmark";
        private const string OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
        private const Int32 DEFAULT_MAX_CHANNELS_PER_TARGET = 10;
        private GrpcGcp.ApiConfig config = new GrpcGcp.ApiConfig();
        private Spanner.SpannerClient client;

        [TestInitialize]
        public void SetUp()
        {
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
            DefaultCallInvoker invoker = new DefaultCallInvoker(TARGET, credential.ToChannelCredentials(), options);
            client = new Spanner.SpannerClient(invoker);
        }

        private void InitApiConfig()
        {
            config.ChannelPool = new GrpcGcp.ChannelPoolConfig();
            config.ChannelPool.MaxConcurrentStreamsLowWatermark = 1;
            config.ChannelPool.MaxSize = 10;
            AddMethod(config, "/google.spanner.v1.Spanner/CreateSession", GrpcGcp.AffinityConfig.Types.Command.Bind, "name");
            AddMethod(config, "/google.spanner.v1.Spanner/GetSession", GrpcGcp.AffinityConfig.Types.Command.Bound, "name");
            AddMethod(config, "/google.spanner.v1.Spanner/DeleteSession", GrpcGcp.AffinityConfig.Types.Command.Unbind, "name");
            AddMethod(config, "/google.spanner.v1.Spanner/ExecuteSql", GrpcGcp.AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/ExecuteStreamingSql", GrpcGcp.AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/Read", GrpcGcp.AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/StreamingRead", GrpcGcp.AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/BeginTransaction", GrpcGcp.AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/Commit", GrpcGcp.AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/Rollback", GrpcGcp.AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/PartitionQuery", GrpcGcp.AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/PartitionRead", GrpcGcp.AffinityConfig.Types.Command.Bound, "session");
        }

        private void AddMethod(GrpcGcp.ApiConfig config, string name, GrpcGcp.AffinityConfig.Types.Command command, string affinityKey)
        {
            GrpcGcp.MethodConfig method = new GrpcGcp.MethodConfig();
            method.Name.Add(name);
            method.Affinity = new GrpcGcp.AffinityConfig();
            method.Affinity.Command = command;
            method.Affinity.AffinityKey = affinityKey;
            config.Method.Add(method);
        }

        [TestMethod]
        public void CreateSessionWithReusedChannel()
        {
            for (int i = 0; i < DEFAULT_MAX_CHANNELS_PER_TARGET; i++)
            {
                Session session;
                {
                    CreateSessionRequest request = new CreateSessionRequest();
                    request.Database = DATABASE;
                    session = client.CreateSession(request);
                    Assert.IsNotNull(session);
                }
                {
                    DeleteSessionRequest request = new DeleteSessionRequest();
                    request.Name = session.Name;
                    client.DeleteSession(request);
                }
            }
        }
    }
}

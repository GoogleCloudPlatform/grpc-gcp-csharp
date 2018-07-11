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
using System.Linq;

namespace Grpc.Gcp.IntegrationTest
{
    [TestClass]
    public class SpannerTest
    {
        private const string TARGET = "spanner.googleapis.com";
        private const string DATABASE = "projects/grpc-gcp/instances/sample/databases/benchmark";
        private const string TABLE = "storage";
        private const string COLUMN_ID_PAYLOAD = "payload";
        private const string OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
        private const Int32 DEFAULT_MAX_CHANNELS_PER_TARGET = 10;
        private ApiConfig config = new ApiConfig();
        private DefaultCallInvoker invoker;
        private Spanner.SpannerClient client;

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
            client = new Spanner.SpannerClient(invoker);
        }

        private void InitApiConfig()
        {
            config.ChannelPool = new ChannelPoolConfig();
            config.ChannelPool.MaxConcurrentStreamsLowWatermark = 1;
            config.ChannelPool.MaxSize = 10;
            AddMethod(config, "/google.spanner.v1.Spanner/CreateSession", AffinityConfig.Types.Command.Bind, "name");
            AddMethod(config, "/google.spanner.v1.Spanner/GetSession", AffinityConfig.Types.Command.Bound, "name");
            AddMethod(config, "/google.spanner.v1.Spanner/DeleteSession", AffinityConfig.Types.Command.Unbind, "name");
            AddMethod(config, "/google.spanner.v1.Spanner/ExecuteSql", AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/ExecuteStreamingSql", AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/Read", AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/StreamingRead", AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/BeginTransaction", AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/Commit", AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/Rollback", AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/PartitionQuery", AffinityConfig.Types.Command.Bound, "session");
            AddMethod(config, "/google.spanner.v1.Spanner/PartitionRead", AffinityConfig.Types.Command.Bound, "session");
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
        public void CreateSessionWithReusedChannel()
        {
            for (int i = 0; i < DEFAULT_MAX_CHANNELS_PER_TARGET * 2; i++)
            {
                Session session;
                {
                    CreateSessionRequest request = new CreateSessionRequest();
                    request.Database = DATABASE;
                    session = client.CreateSession(request);
                    Assert.IsNotNull(session);
                    Assert.AreEqual(1, invoker.channelRefs.Count);
                }
                {
                    DeleteSessionRequest request = new DeleteSessionRequest();
                    request.Name = session.Name;
                    client.DeleteSession(request);
                }
            }
        }

        [TestMethod]
        public void CreateListDeleteSession()
        {
            Session session;
            {
                CreateSessionRequest request = new CreateSessionRequest();
                request.Database = DATABASE;
                session = client.CreateSession(request);
                Assert.IsNotNull(session);
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(1, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
            }

            {
                ListSessionsRequest request = new ListSessionsRequest();
                request.Database = DATABASE;
                ListSessionsResponse response = client.ListSessions(request);
                Assert.IsNotNull(response);
                Assert.IsNotNull(response.Sessions);
                Assert.IsTrue(response.Sessions.Any(item => item.Name == session.Name));
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(1, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
            }

            {
                DeleteSessionRequest request = new DeleteSessionRequest();
                request.Name = session.Name;
                client.DeleteSession(request);
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(0, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
            }

            {
                ListSessionsRequest request = new ListSessionsRequest();
                request.Database = DATABASE;
                ListSessionsResponse response = client.ListSessions(request);
                Assert.IsNotNull(response);
                Assert.IsNotNull(response.Sessions);
                Assert.IsFalse(response.Sessions.Any(item => item.Name == session.Name));
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(0, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
            }
        }

        [TestMethod]
        public void ExecuteSql()
        {
            Session session;
            {
                CreateSessionRequest request = new CreateSessionRequest();
                request.Database = DATABASE;
                session = client.CreateSession(request);
                Assert.IsNotNull(session);
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(1, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
            }
            {
                ExecuteSqlRequest request = new ExecuteSqlRequest();
                request.Session = session.Name;
                request.Sql = string.Format("select id, data from {0}", TABLE);
                ResultSet resultSet = client.ExecuteSql(request);
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(1, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
                Assert.IsNotNull(resultSet);
                Assert.AreEqual(1, resultSet.Rows.Count);
                Assert.AreEqual(COLUMN_ID_PAYLOAD, resultSet.Rows[0].Values[0].StringValue);
            }
            {
                DeleteSessionRequest request = new DeleteSessionRequest();
                request.Name = session.Name;
                client.DeleteSession(request);
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(0, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
            }
        }

        [TestMethod]
        public void ExecuteSqlAsync()
        {
            Session session;
            {
                CreateSessionRequest request = new CreateSessionRequest();
                request.Database = DATABASE;
                session = client.CreateSession(request);
                Assert.IsNotNull(session);
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(1, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
            }
            {
                ExecuteSqlRequest request = new ExecuteSqlRequest();
                request.Session = session.Name;
                request.Sql = string.Format("select id, data from {0}", TABLE);
                AsyncUnaryCall<ResultSet> call = client.ExecuteSqlAsync(request);
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(1, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(1, invoker.channelRefs[0].ActiveStreamRef);

                ResultSet resultSet = call.ResponseAsync.Result;

                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(1, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);

                Assert.IsNotNull(resultSet);
                Assert.AreEqual(1, resultSet.Rows.Count);
                Assert.AreEqual(COLUMN_ID_PAYLOAD, resultSet.Rows[0].Values[0].StringValue);
            }
            {
                DeleteSessionRequest request = new DeleteSessionRequest();
                request.Name = session.Name;
                client.DeleteSession(request);
                Assert.AreEqual(1, invoker.channelRefs.Count);
                Assert.AreEqual(0, invoker.channelRefs[0].AffinityRef);
                Assert.AreEqual(0, invoker.channelRefs[0].ActiveStreamRef);
            }
        }

        [TestMethod]
        public void BoundUnbindInvalidAffinityKey()
        {
            GetSessionRequest getSessionRequest = new GetSessionRequest();
            getSessionRequest.Name = "random_name";
            Assert.ThrowsException<Grpc.Core.RpcException>(() => client.GetSession(getSessionRequest));

            DeleteSessionRequest deleteSessionRequest = new DeleteSessionRequest();
            deleteSessionRequest.Name = "random_name";
            
            Assert.ThrowsException<Grpc.Core.RpcException>(() => client.DeleteSession(deleteSessionRequest));
        }

        [TestMethod]
        public void BoundAfterUnbind()
        {
            CreateSessionRequest request = new CreateSessionRequest();
            request.Database = DATABASE;
            Session session = client.CreateSession(request);

            Assert.AreEqual(1, invoker.channelRefByAffinityKey.Count);

            DeleteSessionRequest deleteSessionRequest = new DeleteSessionRequest();
            deleteSessionRequest.Name = session.Name;
            client.DeleteSession(deleteSessionRequest);

            Assert.AreEqual(0, invoker.channelRefByAffinityKey.Count);

            GetSessionRequest getSessionRequest = new GetSessionRequest();
            getSessionRequest.Name = session.Name;
            Assert.ThrowsException<Grpc.Core.RpcException>(() => client.GetSession(getSessionRequest));

        }

    }
}

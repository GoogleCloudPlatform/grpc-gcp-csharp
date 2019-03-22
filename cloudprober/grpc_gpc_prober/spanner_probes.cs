using System.Collections.Generic;
using System.Diagnostics;
using Grpc.Core;
using Google.Cloud.Spanner.V1;
using ProbeTestsBase;

namespace SpannerProbesTest
{
    public class SpannerProbesTestClass : ProbeTestsBaseClass
    {
        private Dictionary<string, string> probFunctions;
        private static string _DATABASE = "projects/cloudprober-test/instances/test-instance/databases/test-db";
        //private static string _TEST_USERNAME = "test_username";
        static private readonly Stopwatch stopwatch = new Stopwatch();

        /*
         *  Constructor of SpannerProbesTestClass
         *  Initialize probFunctions       
         */

        public SpannerProbesTestClass()
        {
            this.probFunctions = new Dictionary<string, string>()
            {
                {"session_management", "sessionManagement"},
                {"execute_sql", "executeSql"},
                {"read", "read"},
                {"transaction", "transaction"},
                {"partition", "partition"}
            };
        }

        /*
         * Return probFunctions
         */

        public Dictionary<string, string> GetProbFunctions()
        {
            return this.probFunctions;
        }

        /*
         * Create a Session from client
         */

        public static Session StartSession(Spanner.SpannerClient client)
        {
            CreateSessionRequest createSessionRequest = new CreateSessionRequest();
            createSessionRequest.Database = _DATABASE;
            return client.CreateSession(createSessionRequest);
        }

        /*
         * Delete Current Session      
         */      

        public static void EndSession(Spanner.SpannerClient client ,Session session)
        {
            DeleteSessionRequest deleteSessionRequest = new DeleteSessionRequest();
            deleteSessionRequest.Name = session.Name;
            client.DeleteSession(deleteSessionRequest);
            return;
        }

        /*
        Probes to test session related grpc call from Spanner stub.

        Includes tests against CreateSession, GetSession, ListSessions, and
        DeleteSession of Spanner stub.

        Args:
            stub: An object of SpannerStub.
            metrics: A list of metrics.

        */

        public static void sessionManagement(Spanner.SpannerClient client, ref Dictionary<string, long> metrics)
        {
            long latency;
            CreateSessionRequest createSessionRequest = new CreateSessionRequest();
            createSessionRequest.Database = _DATABASE;
            //Create Session test
            //Create
            stopwatch.Start();
            Session session = client.CreateSession(createSessionRequest);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("create_session_latency_ms", latency);

            //Get Session
            GetSessionRequest getSessionRequest = new GetSessionRequest();
            getSessionRequest.Name = session.Name;
            stopwatch.Start();
            client.GetSession(getSessionRequest);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("get_session_latency_ms", latency);

            //List Session
            ListSessionsRequest listSessionsRequest = new ListSessionsRequest();
            listSessionsRequest.Database = _DATABASE;
            stopwatch.Start();
            client.ListSessions(listSessionsRequest);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("list_sessions_latency_ms", latency);

            //Delete Session
            DeleteSessionRequest deleteSessionRequest = new DeleteSessionRequest();
            deleteSessionRequest.Name = session.Name;
            stopwatch.Start();
            client.DeleteSession(deleteSessionRequest);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("delete_session_latency_ms", latency);
        }

        /*
        Probes to test ExecuteSql and ExecuteStreamingSql call from Spanner stub.

        Args:
            stub: An object of SpannerStub.
            metrics: A list of metrics.

        */

        public static void executeSql(Spanner.SpannerClient client, ref Dictionary<string, long>metrics)
        {
            long latency;

            //Create Session
            Session session = StartSession(client);

            //Probing ExecuteSql Call
            stopwatch.Start();
            ExecuteSqlRequest executeSqlRequest = new ExecuteSqlRequest();
            executeSqlRequest.Session = session.Name;
            executeSqlRequest.Sql = "select * FROM users";
            client.ExecuteSql(executeSqlRequest);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("execute_sql_latency_ms", latency);

            //Probing ExecuteStreamingSql Call
            AsyncServerStreamingCall<PartialResultSet> partial_result_set = client.ExecuteStreamingSql(executeSqlRequest);

            stopwatch.Start();
            var header = partial_result_set.ResponseHeadersAsync;
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("execute_streaming_sql_latency_ms", latency);

            //Delete Session
            EndSession(client, session);
        }

        /*
        Probe to test Read and StreamingRead grpc call from Spanner stub.

        Args:
            stub: An object of SpannerStub.
            metrics: A list of metrics.
        */

        public static void read(Spanner.SpannerClient client, ref Dictionary<string, long> metrics)
        {
            long latency;

            //Create Session
            Session session = StartSession(client);

            //Probing Read Call
            stopwatch.Start();
            ReadRequest readRequest = new ReadRequest();
            readRequest.Session = session.Name;
            readRequest.Table = "users";
            KeySet keyset = new KeySet();
            keyset.All = true;
            readRequest.KeySet = keyset;
            client.Read(readRequest);
            stopwatch.Stop();

            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("read_latency_ms", latency);

            //Probing StreamingRead Call
            AsyncServerStreamingCall<PartialResultSet> result_set = client.StreamingRead(readRequest);
            stopwatch.Start();
            var header = result_set.ResponseHeadersAsync;
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("streaming_read_latency_ms", latency);

            //Delete Session
            EndSession(client, session);
        }

        /*
        Probe to test BeginTransaction, Commit and Rollback grpc from Spanner stub.

        Args:
            stub: An object of SpannerStub.
            metrics: A list of metrics.
        */

        public static void transaction(Spanner.SpannerClient client, ref Dictionary<string, long> metrics)
        {
            long latency;

            //Start Session
            Session session = StartSession(client);

            TransactionOptions txn_options = new TransactionOptions();
            TransactionOptions.Types.ReadWrite rw = new TransactionOptions.Types.ReadWrite();
            txn_options.ReadWrite = rw;
            BeginTransactionRequest txn_request = new BeginTransactionRequest();
            txn_request.Session = session.Name;
            txn_request.Options = txn_options;

            //Probing BeginTransaction Call
            stopwatch.Start();
            Transaction txn = client.BeginTransaction(txn_request);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("begin_transaction_latency_ms", latency);

            CommitRequest commitRequest = new CommitRequest();
            commitRequest.Session = session.Name;
            commitRequest.TransactionId = txn.Id;

            //Probing Commit Call
            stopwatch.Start();
            client.Commit(commitRequest);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("commit_latency_ms", latency);

            txn = client.BeginTransaction(txn_request);
            RollbackRequest rollbackRequest = new RollbackRequest();
            rollbackRequest.Session = session.Name;
            rollbackRequest.TransactionId = txn.Id;

            //Probing Rollback Call
            stopwatch.Start();
            client.Rollback(rollbackRequest);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("rollback_latency_ms", latency);

            //Delete Session
            EndSession(client, session);
        }

        /*
        Probe to test PartitionQuery and PartitionRead grpc call from Spanner stub.

        Args:
            stub: An object of SpannerStub.
            metrics: A list of metrics.
        */

        public static void partition(Spanner.SpannerClient client, ref Dictionary<string, long> metrics)
        {
            long latency;

            //Start Session
            Session session = StartSession(client);

            TransactionOptions txn_options = new TransactionOptions();
            TransactionOptions.Types.ReadOnly ro = new TransactionOptions.Types.ReadOnly();
            txn_options.ReadOnly = ro;
            TransactionSelector txn_selector = new TransactionSelector();
            txn_selector.Begin = txn_options;

            //Probing PartitionQuery call
            PartitionQueryRequest ptn_query_request = new PartitionQueryRequest();
            ptn_query_request.Session = session.Name;
            ptn_query_request.Sql = "select * FROM users";
            ptn_query_request.Transaction = txn_selector;

            stopwatch.Start();
            client.PartitionQuery(ptn_query_request);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("partition_query_latency_ms", latency);

            //Probing PartitionRead Call
            PartitionReadRequest ptn_read_request = new PartitionReadRequest();
            ptn_read_request.Session = session.Name;
            ptn_read_request.Table = "users";
            ptn_read_request.Transaction = txn_selector;
            KeySet keyset = new KeySet();
            keyset.All = true;
            ptn_read_request.KeySet = keyset;
            stopwatch.Start();
            client.PartitionRead(ptn_read_request);
            stopwatch.Stop();
            latency = stopwatch.ElapsedMilliseconds;
            metrics.Add("partition_read_latency_ms", latency);

            //Delete Session
            EndSession(client, session);

        }

    }
}
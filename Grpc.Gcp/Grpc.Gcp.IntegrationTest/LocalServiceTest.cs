using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Grpc.Gcp.IntegrationTest
{
    [TestClass]
    public class LocalServiceTest
    {
        [TestMethod]
        public async Task ExceptionPropagation()
        {
            await RunWithServer(
                new ThrowingService(),
                null,
                async (invoker, client) =>
                {
                    await Assert.ThrowsExceptionAsync<RpcException>(async () => await client.DoSimpleAsync(new SimpleRequest()));
                    AssertNoActiveStreams(invoker);
                },
                (invoker, client) =>
                {
                    Assert.ThrowsException<RpcException>(() => client.DoSimple(new SimpleRequest()));
                    AssertNoActiveStreams(invoker);
                });
        }

        private void AssertNoActiveStreams(GcpCallInvoker invoker)
        {
            var channelRefs = invoker.GetChannelRefsForTest();
            Assert.AreEqual(0, channelRefs.Sum(cr => cr.ActiveStreamCount));
        }

        /// <summary>
        /// Starts up a service, creates a call invoker and a client, using an optional API config,
        /// then executes the asynchronous and/or synchronous test methods provided.
        /// </summary>
        private static async Task RunWithServer(
            TestService.TestServiceBase serviceImpl,
            ApiConfig apiConfig,
            Func<GcpCallInvoker, TestService.TestServiceClient, Task> asyncTestAction,
            Action<GcpCallInvoker, TestService.TestServiceClient> testAction)
        {
            var server = new Server
            {
                Services = { TestService.BindService(serviceImpl) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();
            try
            {
                var port = server.Ports.First();
                var options = new List<ChannelOption>();
                if (apiConfig != null)
                {
                    options.Add(new ChannelOption(GcpCallInvoker.ApiConfigChannelArg, apiConfig.ToString()));
                }
                var invoker = new GcpCallInvoker(port.Host, port.BoundPort, ChannelCredentials.Insecure, options);
                var client = new TestService.TestServiceClient(invoker);
                await (asyncTestAction?.Invoke(invoker, client) ?? Task.FromResult(0));
                testAction?.Invoke(invoker, client);
            }
            finally
            {
                await server.ShutdownAsync();
            }
        }

        private class ThrowingService : TestService.TestServiceBase
        {
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
            public override async Task<SimpleResponse> DoSimple(SimpleRequest request, ServerCallContext context)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
            {
                throw new Exception("Bang");
            }
        }
    }
}

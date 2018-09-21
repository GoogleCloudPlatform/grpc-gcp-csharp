using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
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
            var server = new Server
            {
                Services = { TestService.BindService(new ThrowingService()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();
            try
            {
                var port = server.Ports.First();
                var invoker = new GcpCallInvoker(port.Host, port.BoundPort, ChannelCredentials.Insecure);

                var client = new TestService.TestServiceClient(invoker);

                await Assert.ThrowsExceptionAsync<RpcException>(async () => await client.DoSimpleAsync(new SimpleRequest()));
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

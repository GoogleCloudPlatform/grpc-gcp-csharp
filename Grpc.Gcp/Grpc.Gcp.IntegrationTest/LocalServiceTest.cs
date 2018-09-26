using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static Grpc.Gcp.AffinityConfig.Types;

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

        public static IEnumerable<object> InvalidAffinityData => new object[][]
        {
            new object[] { "not_a_field" },
            new object[] { "inner.not_a_field" },
            new object[] { "number" },
            new object[] { "inner" },
            new object[] { "inner.number" },
            new object[] { "inner.nested_inner" },
            new object[] { "inner.nested_inner.number" },
            new object[] { "inner.nested_inner.nested_inner" },
        };

        [TestMethod]
        public void NoChannelPoolConfig()
        {
            var config = new ApiConfig();
            var options = new ChannelOption[] { new ChannelOption(GcpCallInvoker.ApiConfigChannelArg, config.ToString()) };
            Assert.ThrowsException<ArgumentException>(() => new GcpCallInvoker("localhost", 12345, ChannelCredentials.Insecure, options));
        }

        [DataTestMethod]
        [DynamicData(nameof(InvalidAffinityData), DynamicDataSourceType.Property)]
        public async Task InvalidAffinity(string affinityKey)
        {
            ComplexRequest request = new ComplexRequest
            {
                Name = "name",
                Number = 10,
                Inner = new ComplexInner
                {
                    Name = "name",
                    Number = 10,
                    NestedInner = new ComplexInner { NestedInner = new ComplexInner() }
                }
            };

            var config = CreateApiConfig(affinityKey, Command.Bind);
            await RunWithServer(
                new CopyService(),
                config,
                async (invoker, client) =>
                    await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () => await client.DoComplexAsync(request)),
                (invoker, client) =>
                    Assert.ThrowsException<InvalidOperationException>(() => client.DoComplex(request)));
        }

        public static IEnumerable<object> NullOrEmptyAffinityData => new object[][]
        {
            new object[] { "name", new ComplexRequest() },
            new object[] { "inner.name", new ComplexRequest() },
            new object[] { "inner.nested_inner.name", new ComplexRequest() },
            new object[] { "inner.nested_inner.name", new ComplexRequest { Inner = new ComplexInner() } },
        };

        [DataTestMethod]
        [DynamicData(nameof(NullOrEmptyAffinityData), DynamicDataSourceType.Property)]
        public async Task NullOrEmptyAffinity_Bind(string affinityKey, ComplexRequest request)
        {
            var config = CreateApiConfig(affinityKey, Command.Bind);
            await RunWithServer(
                new CopyService(),
                config,
                async (invoker, client) =>
                {
                    await client.DoComplexAsync(request);
                    AssertNoAffinity(invoker);
                },
                (invoker, client) =>
                {
                    client.DoComplex(request);
                    AssertNoAffinity(invoker);
                });

            void AssertNoAffinity(GcpCallInvoker invoker)
            {
                var channelRefs = invoker.GetChannelRefsForTest();
                Assert.AreEqual(0, channelRefs.Sum(cr => cr.ActiveStreamCount));
            }
        }

        [DataTestMethod]
        [DynamicData(nameof(NullOrEmptyAffinityData), DynamicDataSourceType.Property)]
        public async Task NullOrEmptyAffinity_Unbind(string affinityKey, ComplexRequest request)
        {
            var config = CreateApiConfig(affinityKey, Command.Unbind);
            await RunWithServer(
                new CopyService(),
                config,
                async (invoker, client) =>
                {
                    await client.DoComplexAsync(request);
                    AssertNoAffinity(invoker);
                },
                (invoker, client) =>
                {
                    client.DoComplex(request);
                    AssertNoAffinity(invoker);
                });

            void AssertNoAffinity(GcpCallInvoker invoker)
            {
                var channelRefs = invoker.GetChannelRefsForTest();
                Assert.AreEqual(0, channelRefs.Sum(cr => cr.ActiveStreamCount));
            }
        }

        private static ApiConfig CreateApiConfig(string complexAffinityKey, Command command) => new ApiConfig
        {
            ChannelPool = new ChannelPoolConfig { IdleTimeout = 1000, MaxConcurrentStreamsLowWatermark = 10, MaxSize = 10 },
            Method =
            {
                new MethodConfig
                {
                    Name = { "/grpc.gcp.integration_test.TestService/DoComplex" },
                    Affinity = new AffinityConfig { AffinityKey = complexAffinityKey, Command = command }
                }
            }
        };

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

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        private class ThrowingService : TestService.TestServiceBase
        {
            public override async Task<SimpleResponse> DoSimple(SimpleRequest request, ServerCallContext context) =>
                throw new Exception("Bang");

            public override async Task<ComplexResponse> DoComplex(ComplexRequest request, ServerCallContext context) =>
                throw new Exception("Bang");
        }

        private class CopyService : TestService.TestServiceBase
        {
            public override async Task<SimpleResponse> DoSimple(SimpleRequest request, ServerCallContext context) =>
                new SimpleResponse { Name = request.Name };
            public override async Task<ComplexResponse> DoComplex(ComplexRequest request, ServerCallContext context) =>
                new ComplexResponse
                {
                    Name = request.Name,
                    Number = request.Number,
                    Inner = request.Inner
                };
        }
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
    }
}

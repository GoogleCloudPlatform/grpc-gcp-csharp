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

            void AssertNoActiveStreams(GcpCallInvoker invoker)
            {
                var channelRefs = invoker.GetChannelRefsForTest();
                Assert.AreEqual(0, channelRefs.Sum(cr => cr.ActiveStreamCount));
            }
        }

        [TestMethod]
        public void NoChannelPoolConfig()
        {
            var config = new ApiConfig();
            var options = new ChannelOption[] { new ChannelOption(GcpCallInvoker.ApiConfigChannelArg, config.ToString()) };
            Assert.ThrowsException<ArgumentException>(() => new GcpCallInvoker("localhost", 12345, ChannelCredentials.Insecure, options));
        }

        public static IEnumerable<object[]> GetInvalidAffinityData ()
        {
            return AppendCommand("not_a_field")
                .Concat(AppendCommand("name.not_a_field"))
                .Concat(AppendCommand("number"))
                .Concat(AppendCommand("inner"))
                .Concat(AppendCommand("inner.not_a_field"))
                .Concat(AppendCommand("inner.name.not_a_field"))
                .Concat(AppendCommand("inner.number"))
                .Concat(AppendCommand("inner.nested_inner"))
                .Concat(AppendCommand("inner.nested_inner.number"))
                .Concat(AppendCommand("inner.nested_inner.nested_inner"))
                .Concat(AppendCommand("inner.nested_inners"))
                .Concat(AppendCommand("inner.nested_inners.not_a_field"))
                .Concat(AppendCommand("inner.nested_inners.number"))
                .Concat(AppendCommand("inner.nested_inners.nested_inner"));

            IEnumerable<object[]> AppendCommand(string affinityKey)
            {
                yield return new object[] { affinityKey, Command.Bind };
                yield return new object[] { affinityKey, Command.Unbind };
                yield return new object[] { affinityKey, Command.Bound };
            }
        }

        [DataTestMethod]
        [DynamicData(nameof(GetInvalidAffinityData), DynamicDataSourceType.Method)]
        public async Task InvalidAffinity(string affinityKey, Command command)
        {
            ComplexRequest request = new ComplexRequest
            {
                Name = "name",
                Number = 10,
                Inner = new ComplexInner
                {
                    Name = "name",
                    Number = 10,
                    NestedInner = new ComplexInner { NestedInner = new ComplexInner() },
                    NestedInners =
                    {
                        new ComplexInner
                        {
                            Name = "inner name",
                            Number = 5,
                            NestedInner = new ComplexInner { }
                        },
                        new ComplexInner
                        {
                            Name = "another inner name",
                            Number = 15,
                            NestedInner = new ComplexInner { }
                        }
                    }
                }
            };

            var config = CreateApiConfig(affinityKey, command);
            await RunWithServer(
                new CopyService(),
                config,
                async (invoker, client) =>
                    await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () => await client.DoComplexAsync(request)),
                (invoker, client) =>
                    Assert.ThrowsException<InvalidOperationException>(() => client.DoComplex(request)));
        }

        public static IEnumerable<object[]> GetNullOrEmptyAffinityData()
        {
            return AppendCommand("name", new ComplexRequest())
                .Concat(AppendCommand("inner.name", new ComplexRequest()))
                .Concat(AppendCommand("inner.nested_inner.name", new ComplexRequest()))
                .Concat(AppendCommand("inner.nested_inner.name", new ComplexRequest { Inner = new ComplexInner() }))
                .Concat(AppendCommand("inner.nested_inners", new ComplexRequest { Inner = new ComplexInner() }))
                .Concat(AppendCommand("inner.nested_inners.name", new ComplexRequest { Inner = new ComplexInner { NestedInners = { new ComplexInner() }}}))
                // This one only for Bind, since Bound and Unbind don't accept multiple affinity keys. This is tested elsewhere.
                .Concat(new object[][] { new object[] { "inner.nested_inners.name", new ComplexRequest { Inner = new ComplexInner { NestedInners = { new ComplexInner(), new ComplexInner() }}}, Command.Bind }});

            IEnumerable<object[]> AppendCommand(string affinityKey, ComplexRequest request)
            {
                yield return new object[] { affinityKey, request, Command.Bind };
                yield return new object[] { affinityKey, request, Command.Unbind };
                yield return new object[] { affinityKey, request, Command.Bound };
            }
        }

        [DataTestMethod]
        [DynamicData(nameof(GetNullOrEmptyAffinityData), DynamicDataSourceType.Method)]
        public async Task NullOrEmptyAffinity(string affinityKey, ComplexRequest request, Command command)
        {
            var config = CreateApiConfig(affinityKey, command);
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
        }

        public static IEnumerable<object[]> SingleAffinityData => new object[][]
        {
            new object[]
            {
                "name",
                new ComplexRequest
                {
                    Name = "affinityKey1"
                },
                "affinityKey1"
            },
            new object[]
            {
                "inner.name",
                new ComplexRequest
                {
                    Name = "affinityKey1",
                    Inner = new ComplexInner
                    {
                        Name = "affinityKey2"
                    }
                },
                "affinityKey2"
            },
            new object[]
            {
                "inner.nested_inner.name",
                new ComplexRequest
                {
                    Name = "affinityKey1",
                    Inner = new ComplexInner
                    {
                        Name = "affinityKey2",
                        NestedInner = new ComplexInner
                        {
                            Name = "affinityKey3"
                        }
                    }
                },
                "affinityKey3"
            },
            new object[]
            {
                "inner.nested_inners.name",
                new ComplexRequest
                {
                    Name = "affinityKey1",
                    Inner = new ComplexInner
                    {
                        Name = "affinityKey2",
                        NestedInner = new ComplexInner
                        {
                            Name = "affinityKey3"
                        },
                        NestedInners =
                        {
                            new ComplexInner
                            {
                                Name = "affinityKey4"
                            }
                        }
                    }
                },
                "affinityKey4"
            }
        };

        public static IEnumerable<object[]> BatchAffinityData => new object[][]
        {
            new object[] 
            {
                "inner.nested_inners.name",
                new ComplexRequest
                {
                    Inner = new ComplexInner
                    {
                        NestedInners =
                        {
                            new ComplexInner { Name = "nested1"},
                            new ComplexInner { Name = "nested2"},
                            new ComplexInner { Name = "nested3"},
                            new ComplexInner { Name = "nested4"},
                        }
                    }
                }
            },
            new object[]
            {
                "inner.nested_inners.nested_inners.name",
                new ComplexRequest
                {
                    Inner = new ComplexInner
                    {
                        NestedInners =
                        {
                            new ComplexInner
                            {
                                NestedInners =
                                {
                                    new ComplexInner { Name = "nested1"},
                                    new ComplexInner { Name = "nested2"},
                                }
                            },
                            new ComplexInner
                            {
                                NestedInners =
                                {
                                    new ComplexInner { Name = "nested3"},
                                    new ComplexInner { Name = "nested4"},
                                }
                            },
                        }
                    }
                }
            },
        };

        [DataTestMethod]
        [DynamicData(nameof(SingleAffinityData), DynamicDataSourceType.Property)]
        public async Task SingleAffinity_Bind(string affinityKey, ComplexRequest request, string expectedAffinityKey)
        {
            var config = CreateApiConfig(affinityKey, Command.Bind);
            await RunWithServer(
                new CopyService(),
                config,
                async (invoker, client) =>
                {
                    await client.DoComplexAsync(request);
                    AssertAffinity(invoker, 1);
                },
                (invoker, client) =>
                {
                    client.DoComplex(request);
                    // Testing here with 2 due to how the tests are set up.
                    // We use the same test server to test identical sync and async calls.
                    // In relation to affinity and batch affinity binding, two calls will probably never
                    // be the same in reality, because binding should occur on a unique identifier for the
                    // object that is being bound to the channel, on creation of said object.
                    // There's a TODO on the CallInvoker code to check that this actually never happens.
                    // In the meantime, it's not even a bad thing to have these tests as are and this explanation here.
                    AssertAffinity(invoker, 2);
                });

            void AssertAffinity(GcpCallInvoker invoker, int expetedAffinityCount)
            {
                var channelRefs = invoker.GetChannelRefsForTest();
                Assert.AreEqual(expetedAffinityCount, channelRefs.Sum(cr => cr.AffinityCount));

                var affinityKeys = invoker.GetChannelRefsByAffinityKeyForTest().Keys;
                Assert.AreEqual(1, affinityKeys.Count);
                Assert.IsTrue(affinityKeys.Contains(expectedAffinityKey));
            }
        }

        [DataTestMethod]
        [DynamicData(nameof(SingleAffinityData), DynamicDataSourceType.Property)]
        public async Task SingleAffinity_Unbind(string affinityKey, ComplexRequest request, string _)
        {
            var service = new CopyService();
            var bindConfig = CreateApiConfig(affinityKey, Command.Bind);
            var unbindConfig = CreateApiConfig(affinityKey, Command.Unbind);

            // First we need to Bind.
            // Bind works, we test elsewhere.
            Bind();

            // Test for Unbind async
            await RunWithServer(
                service,
                unbindConfig,
                async (invoker, client) =>
                {
                    await client.DoComplexAsync(request);
                    AssertNoAffinity(invoker);
                },
                (invoker, client) =>
                {
                    // No op, we are testing unbind async here.
                    // Can't unbind twice.
                });

            // Bind again so we can test sync unbind
            Bind();

            // Test for Unbind sync
            await RunWithServer(
                service,
                unbindConfig,
                async (invoker, client) =>
                {
                    // No op, we are testing unbind sync here.
                    // Can't unbind twice.
                    await Task.FromResult(0);
                },
                (invoker, client) =>
                {
                    client.DoComplex(request);
                    AssertNoAffinity(invoker);
                });

            async void Bind()
            {
                await RunWithServer(
                    service,
                    bindConfig,
                    async (invoker, client) =>
                    {
                        await client.DoComplexAsync(request);
                    },
                    (invoker, client) =>
                    {
                        // No op, we are just binding to test unbind after.
                        // We are not testing Bind here.
                    });
            }
        }

        [DataTestMethod]
        [DynamicData(nameof(BatchAffinityData), DynamicDataSourceType.Property)]
        public async Task BatchAffinity_Bind(string affinityKey, ComplexRequest request)
        {
            var config = CreateApiConfig(affinityKey, Command.Bind);
            await RunWithServer(
                new CopyService(),
                config,
                async (invoker, client) =>
                {
                    await client.DoComplexAsync(request);
                    AssertBatchAffinity(invoker, 4);
                },
                (invoker, client) =>
                {
                    client.DoComplex(request);
                    // Testing here with 8 due to how the tests are set up.
                    // We use the same test server to test identical sync and async calls.
                    // In relation to affinity and batch affinity binding, two calls will probably never
                    // be the same in reality, because binding should occur on a unique identifier for the
                    // object that is being bound to the channel, on creation of said object.
                    // There's a TODO on the CallInvoker code to check that this actually never happens.
                    // In the meantime, it's not even a bad thing to have these tests as are and this explanation here.
                    AssertBatchAffinity(invoker, 8);
                });

            void AssertBatchAffinity(GcpCallInvoker invoker, int expectedAffinityCount)
            {
                var channelRefs = invoker.GetChannelRefsForTest();
                Assert.AreEqual(expectedAffinityCount, channelRefs.Sum(cr => cr.AffinityCount));

                var affinityKeys = invoker.GetChannelRefsByAffinityKeyForTest().Keys;
                Assert.AreEqual(4, affinityKeys.Count);
                Assert.IsTrue(affinityKeys.Contains("nested1"));
                Assert.IsTrue(affinityKeys.Contains("nested2"));
                Assert.IsTrue(affinityKeys.Contains("nested3"));
                Assert.IsTrue(affinityKeys.Contains("nested4"));
            }
        }

        [DataTestMethod]
        [DynamicData(nameof(BatchAffinityData), DynamicDataSourceType.Property)]
        public async Task BatchAffinity_Unbind(string affinityKey, ComplexRequest request)
        {
            var service = new CopyService();
            var bindConfig = CreateApiConfig(affinityKey, Command.Bind);
            var unbindConfig = CreateApiConfig(affinityKey, Command.Unbind);

            // First we need to Bind.
            // Bind works, we test elsewhere.
            Bind();

            // Test for Unbind async
            await RunWithServer(
                service,
                unbindConfig,
                async (invoker, client) =>
                {
                    await client.DoComplexAsync(request);
                    AssertNoAffinity(invoker);
                },
                (invoker, client) =>
                {
                    // No op, we are testing unbind async here.
                    // Can't unbind twice.
                });

            // Bind again so we can test sync unbind
            Bind();

            // Test for Unbind sync
            await RunWithServer(
                service,
                unbindConfig,
                async (invoker, client) =>
                {
                    // No op, we are testing unbind sync here.
                    // Can't unbind twice.
                    await Task.FromResult(0);
                },
                (invoker, client) =>
                {
                    client.DoComplex(request);
                    AssertNoAffinity(invoker);
                });

            async void Bind()
            {
                await RunWithServer(
                    service,
                    bindConfig,
                    async (invoker, client) =>
                    {
                        await client.DoComplexAsync(request);
                    },
                    (invoker, client) =>
                    {
                        // No op, we are just binding to test unbind after.
                        // We are not testing Bind here.
                    });
            }
        }

        [DataTestMethod]
        [DynamicData(nameof(BatchAffinityData), DynamicDataSourceType.Property)]
        public async Task BatchAffinity_Bound(string affinityKey, ComplexRequest request)
        {
            var config = CreateApiConfig(affinityKey, Command.Bound);
            await RunWithServer(
                new CopyService(),
                config,
                async (invoker, client) =>
                    await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () => await client.DoComplexAsync(request)),
                (invoker, client) =>
                    Assert.ThrowsException<InvalidOperationException>(() => client.DoComplex(request)));
        }

        private void AssertNoAffinity(GcpCallInvoker invoker)
        {
            var channelRefs = invoker.GetChannelRefsForTest();
            Assert.AreEqual(0, channelRefs.Sum(cr => cr.AffinityCount));
            Assert.AreEqual(0, invoker.GetChannelRefsByAffinityKeyForTest().Count);
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

using Google.Protobuf;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Grpc.Gcp
{
    /// <summary>
    /// Invokes client RPCs using <see cref="Calls"/>.
    /// Calls are made through underlying gcp channel pool.
    /// </summary>
    public class GcpCallInvoker : CallInvoker
    {
        public const string ApiConfigChannelArg = "grpc_gcp.api_config";

        internal IDictionary<string, ChannelRef> channelRefByAffinityKey = new Dictionary<string, ChannelRef>();
        internal IList<ChannelRef> channelRefs = new List<ChannelRef>();

        private const string ClientChannelId = "grpc_gcp.client_channel.id";
        private const Int32 DefaultChannelPoolSize = 10;
        private const Int32 DefaultMaxCurrentStreams = 100;
        private Object thisLock = new Object();
        private readonly string target;
        private readonly ApiConfig apiConfig;
        private readonly IDictionary<string, AffinityConfig> affinityByMethod;
        private readonly ChannelCredentials credentials;
        private readonly IEnumerable<ChannelOption> options;

        public GcpCallInvoker(string target, ChannelCredentials credentials, IEnumerable<ChannelOption> options)
        {
            this.target = target;
            this.credentials = credentials;
            this.apiConfig = InitDefaultApiConfig();
            this.affinityByMethod = new Dictionary<string, AffinityConfig>();

            if (options != null)
            {
                ChannelOption option = options.FirstOrDefault(opt => opt.Name == ApiConfigChannelArg);
                if (option != null)
                {
                    apiConfig = ApiConfig.Parser.ParseJson(option.StringValue);
                    affinityByMethod = InitAffinityByMethodIndex(apiConfig);
                }
                this.options = options.Where(o => o.Name != ApiConfigChannelArg).AsEnumerable<ChannelOption>();
            }
        }

        public GcpCallInvoker(string target, ChannelCredentials credentials) : this(target, credentials, null) { }

        public GcpCallInvoker(string host, int port, ChannelCredentials credentials) : this(host, port, credentials, null) { }

        public GcpCallInvoker(string host, int port, ChannelCredentials credentials, IEnumerable<ChannelOption> options) :
            this(string.Format("{0}:{1}", host, port), credentials, options)
        { }

        private ApiConfig InitDefaultApiConfig()
        {
            return new ApiConfig
            {
                ChannelPool = new ChannelPoolConfig
                {
                    MaxConcurrentStreamsLowWatermark = (uint)DefaultMaxCurrentStreams,
                    MaxSize = (uint)DefaultChannelPoolSize
                }
            };
        }

        private IDictionary<string, AffinityConfig> InitAffinityByMethodIndex(ApiConfig config)
        {
            IDictionary<string, AffinityConfig> index = new Dictionary<string, AffinityConfig>();
            if (config != null)
            {
                foreach (MethodConfig method in config.Method)
                {
                    // TODO(fengli): supports wildcard in method selector.
                    foreach (string name in method.Name)
                    {
                        index.Add(name, method.Affinity);
                    }
                }
            }
            return index;
        }

        private ChannelRef GetChannelRef(string affinityKey = null)
        {
            // TODO(fengli): Supports load reporting.
            lock (thisLock)
            {
                if (!string.IsNullOrEmpty(affinityKey))
                {
                    // Finds the gRPC channel according to the affinity key.
                    if (channelRefByAffinityKey.TryGetValue(affinityKey, out ChannelRef channelRef))
                    {
                        return channelRef;
                    }
                    // TODO(fengli): Affinity key not found, log an error.
                }

                // TODO(fengli): Creates new gRPC channels on demand, depends on the load reporting.
                IOrderedEnumerable<ChannelRef> orderedChannelRefs =
                    channelRefs.OrderBy(channelRef => channelRef.ActiveStreamCount);
                foreach (ChannelRef channelRef in orderedChannelRefs)
                {
                    if (channelRef.ActiveStreamCount < apiConfig.ChannelPool.MaxConcurrentStreamsLowWatermark)
                    {
                        // If there's a free channel, use it.
                        return channelRef;
                    }
                    else
                    {
                        //  If all channels are busy, break.
                        break;
                    }
                }
                int count = channelRefs.Count;
                if (count < apiConfig.ChannelPool.MaxSize)
                {
                    // Creates a new gRPC channel.
                    Channel channel = new Channel(target, credentials,
                        options?.Concat(new[] { new ChannelOption(ClientChannelId, count) }));
                    ChannelRef channelRef = new ChannelRef(channel, count);
                    channelRefs.Add(channelRef);
                    return channelRef;
                }
                // If all channels are overloaded and the channel pool is full already,
                // return the channel with least active streams.
                return orderedChannelRefs.First();
            }
        }

        private string GetAffinityKeyFromProto(string affinityKey, IMessage message)
        {
            if (string.IsNullOrEmpty(affinityKey))
            {
                return null;
            }
            string[] names = affinityKey.Split('.');
            if (names != null)
            {
                foreach (string name in names)
                {
                    object value = message.Descriptor.FindFieldByName(name).Accessor.GetValue(message);
                    if (value is string)
                    {
                        return (string)value;
                    }
                    else if (value is IMessage)
                    {
                        message = (IMessage)value;
                    }
                }
            }
            throw new Exception(String.Format("Cannot find the field in the proto, path: {0}", affinityKey));
        }

        private void Bind(ChannelRef channelRef, string affinityKey)
        {
            lock (thisLock)
            {
                if (!string.IsNullOrEmpty(affinityKey) && !channelRefByAffinityKey.Keys.Contains(affinityKey))
                {
                    channelRefByAffinityKey.Add(affinityKey, channelRef);
                }
                channelRefByAffinityKey[affinityKey].AffinityCountIncr();
            }
        }

        private ChannelRef Unbind(string affinityKey)
        {
            lock (thisLock)
            {
                if (channelRefByAffinityKey.TryGetValue(affinityKey, out ChannelRef channelRef))
                {
                    channelRef.AffinityCountDecr();
                    channelRefByAffinityKey.Remove(affinityKey);
                }
                return channelRef;
            }
        }

        private Tuple<ChannelRef, string> PreProcess<TRequest>(AffinityConfig affinityConfig, TRequest request)
        {
            // Gets the affinity bound key if required in the request method.
            string boundKey = null;
            if (affinityConfig != null)
            {
                if (affinityConfig.Command == AffinityConfig.Types.Command.Bound
                    || affinityConfig.Command == AffinityConfig.Types.Command.Unbind)
                {
                    boundKey = GetAffinityKeyFromProto(affinityConfig.AffinityKey, (IMessage)request);
                }
            }
            ChannelRef channelRef = GetChannelRef(boundKey);
            channelRef.ActiveStreamCountIncr();
            return new Tuple<ChannelRef, string>(channelRef, boundKey);
        }

        private void PostProcess<TResponse>(AffinityConfig affinityConfig, ChannelRef channelRef, string boundKey, TResponse response)
        {
            channelRef.ActiveStreamCountDecr();
            // Process BIND or UNBIND if the method has affinity feature enabled.
            if (affinityConfig != null)
            {
                if (affinityConfig.Command == AffinityConfig.Types.Command.Bind)
                {
                    Bind(channelRef, GetAffinityKeyFromProto(affinityConfig.AffinityKey, (IMessage)response));
                }
                else if (affinityConfig.Command == AffinityConfig.Types.Command.Unbind)
                {
                    Unbind(boundKey);
                }
            }
            
        }

        /// <summary>
        /// Invokes a client streaming call asynchronously.
        /// In client streaming scenario, client sends a stream of requests and server responds with a single response.
        /// </summary>
        public override AsyncClientStreamingCall<TRequest, TResponse>
            AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
        {
            // No channel affinity feature for client streaming call.
            ChannelRef channelRef = GetChannelRef();
            var callDetails = new CallInvocationDetails<TRequest, TResponse>(channelRef.Channel, method, host, options);
            var originalCall = Calls.AsyncClientStreamingCall(callDetails);

            TResponse callback(TResponse resp)
            {
                channelRef.ActiveStreamCountDecr();
                return resp;
            }

            // Decrease the active streams count once async response finishes.
            var gcpResponseAsync = originalCall.ResponseAsync
                .ContinueWith(antecendent => callback(antecendent.Result));

            // Create a wrapper of the original AsyncClientStreamingCall.
            return new AsyncClientStreamingCall<TRequest, TResponse>(
                originalCall.RequestStream,
                gcpResponseAsync,
                originalCall.ResponseHeadersAsync,
                () => originalCall.GetStatus(),
                () => originalCall.GetTrailers(),
                () => originalCall.Dispose());
        }

        /// <summary>
        /// Invokes a duplex streaming call asynchronously.
        /// In duplex streaming scenario, client sends a stream of requests and server responds with a stream of responses.
        /// The response stream is completely independent and both side can be sending messages at the same time.
        /// </summary>
        public override AsyncDuplexStreamingCall<TRequest, TResponse>
            AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
        {
            // No channel affinity feature for duplex streaming call.
            ChannelRef channelRef = GetChannelRef();
            var callDetails = new CallInvocationDetails<TRequest, TResponse>(channelRef.Channel, method, host, options);
            var originalCall = Calls.AsyncDuplexStreamingCall(callDetails);

            // Decrease the active streams count once the streaming response finishes its final batch.
            var gcpResponseStream = new GcpClientResponseStream<TRequest, TResponse>(
                originalCall.ResponseStream,
                (resp) => channelRef.ActiveStreamCountDecr());

            // Create a wrapper of the original AsyncDuplexStreamingCall.
            return new AsyncDuplexStreamingCall<TRequest, TResponse>(
                originalCall.RequestStream,
                gcpResponseStream,
                originalCall.ResponseHeadersAsync,
                () => originalCall.GetStatus(),
                () => originalCall.GetTrailers(),
                () => originalCall.Dispose());
        }

        /// <summary>
        /// Invokes a server streaming call asynchronously.
        /// In server streaming scenario, client sends on request and server responds with a stream of responses.
        /// </summary>
        public override AsyncServerStreamingCall<TResponse>
            AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            affinityByMethod.TryGetValue(method.FullName, out AffinityConfig affinityConfig);

            Tuple<ChannelRef, string> tupleResult = PreProcess(affinityConfig, request);
            ChannelRef channelRef = tupleResult.Item1;
            string boundKey = tupleResult.Item2;

            var callDetails = new CallInvocationDetails<TRequest, TResponse>(channelRef.Channel, method, host, options);
            var originalCall = Calls.AsyncServerStreamingCall(callDetails, request);

            // Executes affinity postprocess once the streaming response finishes its final batch.
            var gcpResponseStream = new GcpClientResponseStream<TRequest, TResponse>(
                originalCall.ResponseStream,
                (resp) => PostProcess(affinityConfig, channelRef, boundKey, resp));

            // Create a wrapper of the original AsyncServerStreamingCall.
            return new AsyncServerStreamingCall<TResponse>(
                gcpResponseStream,
                originalCall.ResponseHeadersAsync,
                () => originalCall.GetStatus(),
                () => originalCall.GetTrailers(),
                () => originalCall.Dispose());

        }

        /// <summary>
        /// Invokes a simple remote call asynchronously.
        /// </summary>
        public override AsyncUnaryCall<TResponse>
            AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            affinityByMethod.TryGetValue(method.FullName, out AffinityConfig affinityConfig);

            Tuple<ChannelRef, string> tupleResult = PreProcess(affinityConfig, request);
            ChannelRef channelRef = tupleResult.Item1;
            string boundKey = tupleResult.Item2;

            var callDetails = new CallInvocationDetails<TRequest, TResponse>(channelRef.Channel, method, host, options);
            var originalCall = Calls.AsyncUnaryCall(callDetails, request);

            TResponse callback(TResponse resp)
            {
                PostProcess(affinityConfig, channelRef, boundKey, resp);
                return resp;
            }

            // Executes affinity postprocess once the async response finishes.
            var gcpResponseAsync = originalCall.ResponseAsync
                .ContinueWith(antecendent => callback(antecendent.Result));

            // Create a wrapper of the original AsyncUnaryCall.
            return new AsyncUnaryCall<TResponse>(
                gcpResponseAsync,
                originalCall.ResponseHeadersAsync,
                () => originalCall.GetStatus(),
                () => originalCall.GetTrailers(),
                () => originalCall.Dispose());

        }

        /// <summary>
        /// Invokes a simple remote call in a blocking fashion.
        /// </summary>
        public override TResponse
            BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            affinityByMethod.TryGetValue(method.FullName, out AffinityConfig affinityConfig);

            Tuple<ChannelRef, string> tupleResult = PreProcess(affinityConfig, request);
            ChannelRef channelRef = tupleResult.Item1;
            string boundKey = tupleResult.Item2;

            var callDetails = new CallInvocationDetails<TRequest, TResponse>(channelRef.Channel, method, host, options);
            TResponse response = Calls.BlockingUnaryCall<TRequest, TResponse>(callDetails, request);

            PostProcess(affinityConfig, channelRef, boundKey, response);
            return response;
        }

        /// <summary>
        /// Shuts down the all channels in the underlying channel pool cleanly. It is strongly
        /// recommended to shutdown all previously created channels before exiting from the process.
        /// </summary>
        public async Task ShutdownAsync()
        {
            for (int i = 0; i < channelRefs.Count; i++)
            {
                await channelRefs[i].Channel.ShutdownAsync();
            }
        }
    }
}
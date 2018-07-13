using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core.Interceptors;
using Grpc.Core.Utils;
using System.Threading;

namespace Grpc.Gcp
{
    class ChannelRef
    {
        private int affinityRef;
        private int activeStreamRef;
        private Channel channel;
        private int id;

        public ChannelRef(Channel channel, int id, int affinityRef = 0, int activeStreamRef = 0)
        {
            this.channel = channel;
            this.id = id;
            this.affinityRef = affinityRef;
            this.activeStreamRef = activeStreamRef;
        }

        public void AffinityRefIncr()
        {
            affinityRef++;
        }

        public void AffinityRefDecr()
        {
            affinityRef--;
        }

        public int AffinityRef
        {
            get
            {
                return affinityRef;
            }
        }

        public void ActiveStreamRefIncr()
        {
            activeStreamRef++;
        }

        public void ActiveStreamRefDecr()
        {
            activeStreamRef--;
        }

        public int ActiveStreamRef
        {
            get
            {
                return activeStreamRef;
            }
        }

        public Channel Channel
        {
            get { return channel; }
        }

    }

    public class DefaultCallInvoker : CallInvoker
    {
        public static readonly string API_CONFIG_CHANNEL_ARG = "grpc_gcp.api_config";
        private static readonly string CLIENT_CHANNEL_ID = "grpc_gcp.client_channel.id";
        private readonly ApiConfig config;
        private readonly IDictionary<string, AffinityConfig> affinityByMethod;
        private Object thisLock = new Object();
        internal IDictionary<string, ChannelRef> channelRefByAffinityKey = new Dictionary<string, ChannelRef>();
        internal IList<ChannelRef> channelRefs = new List<ChannelRef>();
        private readonly string target;
        private readonly ChannelCredentials credentials;
        private readonly IEnumerable<ChannelOption> options;

        public DefaultCallInvoker(string target, ChannelCredentials credentials) : this(target, credentials, null) { }

        public DefaultCallInvoker(string target, ChannelCredentials credentials, IEnumerable<ChannelOption> options)
        {
            this.target = target;
            this.credentials = credentials;

            if (options != null)
            {
                ChannelOption option = options.FirstOrDefault(opt => opt.Name == API_CONFIG_CHANNEL_ARG);
                if (option != null)
                {
                    config = ApiConfig.Parser.ParseJson(option.StringValue);
                    affinityByMethod = InitAffinityByMethodIndex(config);
                }
                this.options = options.Where(o => o.Name != API_CONFIG_CHANNEL_ARG).AsEnumerable<ChannelOption>();
            }
        }

        public DefaultCallInvoker(string host, int port, ChannelCredentials credentials) : this(host, port, credentials, null) { }

        public DefaultCallInvoker(string host, int port, ChannelCredentials credentials, IEnumerable<ChannelOption> options) :
            this(string.Format("{0}:{1}", host, port), credentials, options)
        { }

        private class GcpClientResponseStream<TRequest, TResponse> : IAsyncStreamReader<TResponse>
            where TRequest : class
            where TResponse : class
        {
            bool callbackDone = false;
            readonly IAsyncStreamReader<TResponse> originalStreamReader;
            TResponse lastResponse;
            Func<TResponse, TResponse> callback;

            public GcpClientResponseStream(IAsyncStreamReader<TResponse> originalStreamReader, Func<TResponse, TResponse> callback)
            {
                this.originalStreamReader = originalStreamReader;
                this.callback = callback;
            }

            public TResponse Current
            {
                get
                {
                    TResponse current = originalStreamReader.Current;
                    // Record the last response.
                    lastResponse = current;
                    return current;
                }
            }

            public async Task<bool> MoveNext(CancellationToken token)
            {
                bool result = await originalStreamReader.MoveNext(token);

                // The last invokcation of originalStreamReader.MoveNext returns false if finishes successfully.
                if (!result && !callbackDone) 
                {
                    // if stream is successfully proceesed, execute callback and make sure callback is called only once.
                    callback(lastResponse);
                    callbackDone = true;
                }
                return result;
            }

            public void Dispose()
            {
                originalStreamReader.Dispose();
            }
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
                    ChannelRef channelRef;
                    if (channelRefByAffinityKey.TryGetValue(affinityKey, out channelRef))
                    {
                        return channelRef;
                    }
                    // TODO(fengli): Affinity key not found, log an error.
                }

                // TODO(fengli): Creates new gRPC channels on demand, depends on the load reporting.
                IOrderedEnumerable<ChannelRef> orderedChannelRefs =
                    channelRefs.OrderBy(channelRef => channelRef.ActiveStreamRef);
                foreach (ChannelRef channelRef in orderedChannelRefs)
                {
                    if (channelRef.ActiveStreamRef < config.ChannelPool.MaxConcurrentStreamsLowWatermark)
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
                if (count < config.ChannelPool.MaxSize)
                {
                    // Creates a new gRPC channel.
                    Channel channel = new Channel(target, credentials,
                        options.Concat(new[] { new ChannelOption(CLIENT_CHANNEL_ID, count) }));
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
                channelRefByAffinityKey[affinityKey].AffinityRefIncr();
            }
        }

        private ChannelRef Unbind(string affinityKey)
        {
            lock (thisLock)
            {
                ChannelRef channelRef = null;
                if (channelRefByAffinityKey.TryGetValue(affinityKey, out channelRef))
                {
                    channelRef.AffinityRefDecr();
                    channelRefByAffinityKey.Remove(affinityKey);
                }
                return channelRef;
            }
        }

        private Tuple<ChannelRef, string> PreProcess<TRequest>(AffinityConfig affinityConfig, TRequest request)
        {
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
            channelRef.ActiveStreamRefIncr();
            return new Tuple<ChannelRef, string>(channelRef, boundKey);
        }

        private void PostProcess<TResponse>(AffinityConfig affinityConfig, ChannelRef channelRef, string boundKey, TResponse response)
        {
            channelRef.ActiveStreamRefDecr();
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

        public override AsyncClientStreamingCall<TRequest, TResponse>
            AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
        {
            throw new System.NotImplementedException();
        }

        public override AsyncDuplexStreamingCall<TRequest, TResponse>
            AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
        {
            throw new System.NotImplementedException();
        }

        public override AsyncServerStreamingCall<TResponse>
            AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            AffinityConfig affinityConfig;
            affinityByMethod.TryGetValue(method.FullName, out affinityConfig);

            Tuple<ChannelRef, string> tupleResult = PreProcess(affinityConfig, request);
            ChannelRef channelRef = tupleResult.Item1;
            string boundKey = tupleResult.Item2;

            var call = new CallInvocationDetails<TRequest, TResponse>(channelRef.Channel, method, host, options);
            AsyncServerStreamingCall<TResponse> originalCall = Calls.AsyncServerStreamingCall(call, request);

            Func<TResponse, TResponse> callback = (resp) =>
            {
                PostProcess(affinityConfig, channelRef, boundKey, resp);
                return resp;
            };

            var gcpResponseStream = new GcpClientResponseStream<TRequest, TResponse>(originalCall.ResponseStream, callback);

            return new AsyncServerStreamingCall<TResponse>(
                gcpResponseStream,
                originalCall.ResponseHeadersAsync,
                () => originalCall.GetStatus(),
                () => originalCall.GetTrailers(),
                () => originalCall.Dispose());

        }

        public override AsyncUnaryCall<TResponse>
            AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            AffinityConfig affinityConfig;
            affinityByMethod.TryGetValue(method.FullName, out affinityConfig);

            Tuple<ChannelRef, string> tupleResult = PreProcess(affinityConfig, request);
            ChannelRef channelRef = tupleResult.Item1;
            string boundKey = tupleResult.Item2;

            var call = new CallInvocationDetails<TRequest, TResponse>(channelRef.Channel, method, host, options);
            AsyncUnaryCall<TResponse> originalAsyncUnaryCall = Calls.AsyncUnaryCall(call, request);

            Func<TResponse, TResponse> callback = (resp) =>
            {
                PostProcess(affinityConfig, channelRef, boundKey, resp);
                return resp;
            };

            var gcpResponseAsync = originalAsyncUnaryCall.ResponseAsync
                .ContinueWith(antecendent => callback(antecendent.Result));

            return new AsyncUnaryCall<TResponse>(
                gcpResponseAsync,
                originalAsyncUnaryCall.ResponseHeadersAsync,
                () => originalAsyncUnaryCall.GetStatus(),
                () => originalAsyncUnaryCall.GetTrailers(),
                () => originalAsyncUnaryCall.Dispose());

        }

        public override TResponse
            BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            AffinityConfig affinityConfig;
            affinityByMethod.TryGetValue(method.FullName, out affinityConfig);

            Tuple<ChannelRef, string> tupleResult = PreProcess(affinityConfig, request);
            ChannelRef channelRef = tupleResult.Item1;
            string boundKey = tupleResult.Item2;

            var call = new CallInvocationDetails<TRequest, TResponse>(channelRef.Channel, method, host, options);
            TResponse response = Calls.BlockingUnaryCall<TRequest, TResponse>(call, request);

            PostProcess(affinityConfig, channelRef, boundKey, response);
            return response;
        }

        // TODO(weiranfang): Expose method that shuts down all channels in the channel pool.
    }
}
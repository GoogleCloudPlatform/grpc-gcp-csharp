using Grpc.Core;
using System.Threading;

namespace Grpc.Gcp
{
    /// <summary>
    /// Keeps record of channel affinity and active streams.
    /// This class is thread-safe.
    /// </summary>
    internal sealed class ChannelRef
    {
        private int affinityCount;
        private int activeStreamCount;
        private int id;

        public ChannelRef(ChannelBase channel, int id, int affinityCount = 0, int activeStreamCount = 0)
        {
            Channel = channel;
            CallInvoker = channel.CreateCallInvoker();
            this.id = id;
            this.affinityCount = affinityCount;
            this.activeStreamCount = activeStreamCount;
        }

        internal ChannelBase Channel { get; }
        internal CallInvoker CallInvoker { get; }
        internal int AffinityCount => Interlocked.CompareExchange(ref affinityCount, 0, 0);
        internal int ActiveStreamCount => Interlocked.CompareExchange(ref activeStreamCount, 0, 0);

        internal int AffinityCountIncr() => Interlocked.Increment(ref affinityCount);
        internal int AffinityCountDecr() => Interlocked.Decrement(ref affinityCount);
        internal int ActiveStreamCountIncr() => Interlocked.Increment(ref activeStreamCount);
        internal int ActiveStreamCountDecr() => Interlocked.Decrement(ref activeStreamCount);

        internal ChannelRef Clone() => new ChannelRef(Channel, id, AffinityCount, ActiveStreamCount);
    }
}
 
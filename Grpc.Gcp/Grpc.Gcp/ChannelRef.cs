using Grpc.Core;

namespace Grpc.Gcp
{
    /// <summary>
    /// Keeps record of channel affinity and active streams.
    /// </summary>
    internal class ChannelRef
    {
        private int affinityCount;
        private int activeStreamCount;
        private Channel channel;
        private int id;

        public ChannelRef(Channel channel, int id, int affinityCount = 0, int activeStreamCount = 0)
        {
            this.channel = channel;
            this.id = id;
            this.affinityCount = affinityCount;
            this.activeStreamCount = activeStreamCount;
        }

        public void AffinityCountIncr()
        {
            affinityCount++;
        }

        public void AffinityCountDecr()
        {
            affinityCount--;
        }

        public int AffinityCount
        {
            get
            {
                return affinityCount;
            }
        }

        public void ActiveStreamCountIncr()
        {
            activeStreamCount++;
        }

        public void ActiveStreamCountDecr()
        {
            activeStreamCount--;
        }

        public int ActiveStreamCount
        {
            get
            {
                return activeStreamCount;
            }
        }

        public Channel Channel
        {
            get { return channel; }
        }

    }
}

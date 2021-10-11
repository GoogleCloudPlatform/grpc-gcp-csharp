using Grpc.Core;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using Grpc.Net.Client;

namespace Grpc.Gcp
{
    /// <summary>
    /// Encapsulates creation of new channels.
    /// </summary>
    internal sealed class ChannelRefFactoryGrpcDotnet : ChannelRefFactory
    {
        private readonly string target;

        public ChannelRefFactoryGrpcDotnet(string target)
        {
            this.target = target;
        }

        public override ChannelRef CreateChannelRef(int id)
        {
            // TODO: add a message to log
            //GrpcEnvironment.Logger.Info("Grpc.Gcp creating new channel");
            
            // TODO: always use a separate httphandler, so that separate connection is guaranteed.
            // TODO: allow setting GrpcChannelOptions (grpc-dotnet specific channel configuration)
            var channel = GrpcChannel.ForAddress(target);
            return new ChannelRef(channel, id);
        }
    }
}

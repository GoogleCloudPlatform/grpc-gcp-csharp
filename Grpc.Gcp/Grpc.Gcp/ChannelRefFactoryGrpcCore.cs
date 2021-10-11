using Grpc.Core;
using System.Threading;
using System.Collections.Generic;
using System.Linq;

namespace Grpc.Gcp
{
    // /// <summary>
    // /// Encapsulates creation of new channels.
    // /// </summary>
    // internal sealed class ChannelRefFactoryGrpcCore : ChannelRefFactory
    // {
    //     private const string ClientChannelId = "grpc_gcp.client_channel.id";
    //     // TODO: in theory the counter can overflow
    //     private static int clientChannelIdCounter = 0;

    //     private readonly string target;
    //     private readonly ChannelCredentials credentials;
    //     private readonly IEnumerable<ChannelOption> channelOptions;

    //     public ChannelRefFactoryGrpcCore(string target, ChannelCredentials credentials, IEnumerable<ChannelOption> channelOptions)
    //     {
    //         this.target = target;
    //         this.credentials = credentials;
    //         this.channelOptions = channelOptions;
    //     }

    //     public override ChannelRef CreateChannelRef(int id)
    //     {
    //         // Creates a new gRPC channel.
    //         GrpcEnvironment.Logger.Info("Grpc.Gcp creating new channel");

    //         // TODO: clientChannelIdCounter is independent of ID and is basically just a unique identifier
    //         // TODO: is passing the base options needed?
    //         var channel = new Channel(target, credentials,
    //             channelOptions.Concat(new[] { new ChannelOption(ClientChannelId, Interlocked.Increment (ref clientChannelIdCounter)) }));
    //         return new ChannelRef(channel, id);
    //     }
    // }
}

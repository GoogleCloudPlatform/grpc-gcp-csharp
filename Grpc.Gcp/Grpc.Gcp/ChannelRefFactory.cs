using Grpc.Core;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using System;

namespace Grpc.Gcp
{
    /// <summary>
    /// Encapsulates creation of new channels.
    /// </summary>
    public abstract class ChannelRefFactory
    {
        internal virtual ChannelRef CreateChannelRef(int id)
        {
            throw new NotImplementedException();
        }
    }
}

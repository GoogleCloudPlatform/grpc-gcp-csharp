using Grpc.Core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Grpc.Gcp
{
    // A wrapper class for executing post process for server streaming responses.
    internal class GcpClientResponseStream<TRequest, TResponse> : IAsyncStreamReader<TResponse>
        where TRequest : class
        where TResponse : class
    {
        bool callbackDone = false;
        readonly IAsyncStreamReader<TResponse> originalStreamReader;
        TResponse lastResponse;
        Action<TResponse> postProcess;

        public GcpClientResponseStream(IAsyncStreamReader<TResponse> originalStreamReader, Action<TResponse> postProcess)
        {
            this.originalStreamReader = originalStreamReader;
            this.postProcess = postProcess;
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
                postProcess(lastResponse);
                callbackDone = true;
            }
            return result;
        }

        public void Dispose()
        {
            originalStreamReader.Dispose();
        }
    }
}

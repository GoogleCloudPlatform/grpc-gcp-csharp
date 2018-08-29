using Grpc.Core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Grpc.Gcp
{
    /// <summary>
    ///  A wrapper class for handling post process for server streaming responses.
    /// </summary>
    /// <typeparam name="TRequest">The type representing the request.</typeparam>
    /// <typeparam name="TResponse">The type representing the response.</typeparam>
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

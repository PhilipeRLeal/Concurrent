using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Concurrent.UnitTest.Tests
{
    public static class Teste2
    {
        [Fact]
        public static void Run()
        {
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = -1; // -1 is for unlimited. 1 is for sequential.
            var resultado = new ConcurrentDictionary<int, string>();
            
            try
            {
                int N = 100;
                var seq = Enumerable.Range(0, N);
                Parallel.For(
                        0,
                        N,
                        options,
                        (i) =>
                        {
                            var key = $"Thread ={Thread.CurrentThread.ManagedThreadId}";
                            resultado.TryAdd(seq.ElementAt(i), key);
                        }
                    );

                if( Enumerable.Range(0, N).All(x => resultado.Keys.Contains(x)) )
                {
                    Console.WriteLine("SUCCESS! Teste 2 is consistent");
                    Assert.True(true);
                }
                else
                {
                    Console.WriteLine("Routine 2 is not consistent");
                    var message = $"Missing elements: {string.Join(";", Enumerable.Range(0, N).Where(x => !resultado.Keys.Contains(x)))}";
                    Console.WriteLine(message);
                    Assert.Fail(message);
                }
            }
            // No exception is expected in this example, but if one is still thrown from a task,
            // it will be wrapped in AggregateException and propagated to the main thread.
            catch ( AggregateException e )
            {
                var message = $"Parallel.For has thrown the following (unexpected) exception:\n{e}";
                Console.WriteLine(message);
                Console.WriteLine("Routine 2 is not consistent");

                // Este método não deve retornar verdadeiro
                Assert.Fail(message);
            }
        }
    }
}

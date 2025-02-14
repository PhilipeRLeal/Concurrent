using System.Collections.Concurrent;

namespace Concurrent.UnitTest.Tests
{
    public static class Teste2
    {
        [Fact]
        public static void Run()
        {
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = -1; // -1 is for unlimited. 1 is for sequential.
            var resultado = new ConcurrentDictionary<string, int>();
            try
            {
                int N = 100;

                Parallel.For(
                        0,
                        N,
                        options,
                        (i) =>
                        {
                            var key = $"Thread ={Thread.CurrentThread.ManagedThreadId}";
                            lock( resultado )
                            {
                                if ( !resultado.ContainsKey(key) )
                                {
                                    resultado.TryAdd(key, i);
                                }
                            }
                        }
                    );

                if( Enumerable.Range(0, N).All(x => resultado.Values.Contains(x)) )
                {
                    Console.WriteLine("SUCCESS! Teste 2 is consistent");
                }
                else{
                    Console.WriteLine("Routine 2 is not consistent");
                    Console.WriteLine($"Missing elements: {string.Join(";", Enumerable.Range(0, N).Where(x => !resultado.Values.Contains(x)))}");
                }
            }
            // No exception is expected in this example, but if one is still thrown from a task,
            // it will be wrapped in AggregateException and propagated to the main thread.
            catch ( AggregateException e )
            {
                Console.WriteLine("Parallel.For has thrown the following (unexpected) exception:\n{0}", e);
                Console.WriteLine("Routine 2 is not consistent");

                // Este método não deve retornar verdadeiro
                Assert.False(true);
            }
        }
    }
}

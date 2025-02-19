using System.Diagnostics;

namespace Concurrent.UnitTest.Tests
{
    public class MapReducerTester
    {
        [Fact]
        public void TestMapReduce_With_Sum()
        {
            Console.WriteLine("\nTeste MapReduce");

            var enumerable = Enumerable.Range(0,100);

            Func<int, bool> filter = valor => valor % 2 == 0;

            Func<int, int> mapper = valor => valor *3 + 10;

            Func<int, int, int> reducer = (acc, x) => Interlocked.Add(ref acc, x);

            int seed = 0;

            // Running the test several times in order to ensure the result is consistent.
            for ( int i = 0 ; i < 10_000 ; i++ )
            {
                var resultado = MapReducer.MapReduce(enumerable, filter, mapper, reducer, seed);

                int expected = enumerable.Where(filter).Select(mapper).Sum();

                Assert.Equal(expected, resultado);
            }
        }

        /// <summary>
        /// Not clear yet why the results are not consistent between individual runs.
        /// Perhaps, there is some problem with the reducer operation.
        /// </summary>

        [Fact]
        public void TestMaximumValue()
        {
            Console.WriteLine("\nTeste Maximum Value From MapReduce");
            Func<int, int, int>? reducer = (x,y) => {

                if (x <= y)
                {
                    return y;
                }
                else
                {
                    return x;
                }
            };

            Func<int, int> mapper = value =>
            {
                // lets add a sleeping function here to mimic some heavy processing...

                Thread.Sleep(5);

                var result =    value * 3 + 10;

                return result;
            };

            Func<int, bool> filter = value => value % 2 == 0;

            var input = Enumerable.Range(0, 1_000).ToList();
            Stopwatch sw = Stopwatch.StartNew();
            sw.Start();
            var expected = input.Where(filter).Select(mapper).Aggregate(reducer);
            sw.Stop();
            var timeTakenInSerial = (double)sw.ElapsedMilliseconds;
            sw.Reset();

            /// Running the test several times in order to ensure that it is not a random result.
            int maxRuns = 10_000;
            for (int i=0 ; i < maxRuns ; i++)
            {
                sw.Start();
                var resultado = MapReducer.MapReduce(input, filter, mapper, reducer);
                sw.Stop();
                var parallelTimeTaken = (double) sw.ElapsedMilliseconds;
                sw.Reset();
                Console.WriteLine("\n\n\n");
                Console.WriteLine($"Test {i} out of {maxRuns}");
                Console.WriteLine($"    Serial processing is faster than Parallel? {timeTakenInSerial < parallelTimeTaken}");
                Console.WriteLine($"    Ratio Serial/Parallel: {timeTakenInSerial / parallelTimeTaken}");
                Console.WriteLine("\n\n\n");
                Assert.Equal(expected, resultado);
            }
        }

        [Fact]
        public void TestSpeed()
        {
            var N = 1_000;

            var lista = Enumerable.Range(0, N).ToList();
            Func<int, bool> filtro = (valor) => valor % 2 == 0;
            Func<int, int> transform = valor =>
            {
                Thread.Sleep(1);
                return valor *3 + 10;
            };
            
            // A reducer that increases the sequence of elements by one, and this one element is 
            // recoverable due to its unique value.
            Func<List<int>, List<int>> threadReducer = (lista) => 
            {
                lista.Add(-20);
                
                return lista;
            };

            Func<List<int>, List<int>> globalReducer = (lista) => lista;

            var startTime = Stopwatch.GetTimestamp();
            var resultadoEmParalelo = MapReducer.MapReduce(lista, filtro, transform, threadReducer, globalReducer).Order().ToList();
            var endTime  = Stopwatch.GetTimestamp();

            var elapsedTimeInParalel = endTime - startTime;

            startTime = Stopwatch.GetTimestamp();
            var resultadoSerial = lista.Where(filtro).Select(transform).Order().ToList();
            endTime = Stopwatch.GetTimestamp();

            var elapsedTimeInSerial = endTime - startTime;

            //First, let's detect the value that were inserted during the partial reduce operation
            Assert.Contains(-20, resultadoEmParalelo);

            // Later, let's remove this inserted value for sequence comparison
            while( resultadoEmParalelo .Contains(-20))
            {
                var condition = resultadoEmParalelo.Remove(-20);

                Assert.True(condition);
            }

            Assert.Equal(resultadoEmParalelo, resultadoSerial);

            var elapsedDif = elapsedTimeInParalel - elapsedTimeInSerial;

            Assert.True(elapsedDif < 0);

        }
    }
}
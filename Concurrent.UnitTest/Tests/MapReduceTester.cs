using System.Diagnostics;

namespace Concurrent.UnitTest.Tests
{
    public class MapReduceTester
    {
        [Fact]
        public void Test()
        {
            Console.WriteLine("\nTeste MapReduce");
            var resultado = MapReduce.Run(Enumerable.Range(0,10), valor => valor % 2 == 0, valor => valor *3 + 10, null);
            var r = resultado.Select(x => (x-10)/3).Order().ToList();

            Assert.Equal(resultado.Order(), new List<int> { 10, 16, 22, 28, 34 });

        }

        /// <summary>
        /// Not clear yet how to implement a Maximum Reduce operation in Parallel.
        /// </summary>

        [Fact]
        public void TestMaximumValue()
        {
            Console.WriteLine("\nTeste Maximum Value From MapReduce");
            Func<List<int>, List<int>>? reducer = (input) => {

                var maxValue = int.MinValue;

                foreach (var x in input)
                {
                    if (maxValue < x)
                    {
                        maxValue = x;
                    }
                }
                
                return new List<int> (maxValue);
            };

            Func<int, int> mapper = value => value * 3 + 10;

            Func<int, bool> filter = value => value % 2 == 0;

            var input = Enumerable.Range(0,10).ToList();

            var resultado = MapReduce.Run(input, filter, mapper, reducer);

            Assert.Equal(resultado.Order(), new List<int> {40});

        }


        [Fact]
        public void TestSpeed()
        {
            var N = 10_000;

            var lista = Enumerable.Range(0, N).ToList();
            Func<int, bool> filtro = (valor) => valor % 2 == 0;
            Func<int, int> transform = valor =>
            {
                Thread.Sleep(1);
                return valor *3 + 10;
            };
            var startTime = Stopwatch.GetTimestamp();
            var resultadoEmParalelo = MapReduce.Run(lista, filtro, transform, null);
            var endTime  = Stopwatch.GetTimestamp();

            var elapsedTimeInParalel = endTime - startTime;

            startTime = Stopwatch.GetTimestamp();
            var resultadoSerial = lista.Where(filtro).Select(transform).ToList();
            endTime = Stopwatch.GetTimestamp();

            var elapsedTimeInSerial = endTime - startTime;

            Assert.Equal(resultadoEmParalelo.Order().ToList(), resultadoSerial.Order().ToList());

            var elapsedDif = elapsedTimeInParalel - elapsedTimeInSerial;

            Assert.True(elapsedDif < 0);

        }
    }
}
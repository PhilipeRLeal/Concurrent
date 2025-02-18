using System.Diagnostics;

namespace Concurrent.UnitTest.Tests
{
    public class MapReduceTester
    {
        [Fact]
        public void Test()
        {
            Console.WriteLine("\nTeste MapReduce");
            var resultado = MapReduce.Run(Enumerable.Range(0,10), valor => valor % 2 == 0, valor => valor *3 + 10, null, null);
            var r = resultado.Select(x => (x-10)/3).Order().ToList();

            Assert.Equal(resultado.Order(), new List<int> { 10, 16, 22, 28, 34 });

        }

        /// <summary>
        /// Not clear yet why the results are not consistent between individual runs.
        /// Perhaps, there is some problem with the reducer operation.
        /// </summary>

        [Fact]
        public void TestMaximumValue()
        {
            Console.WriteLine("\nTeste Maximum Value From MapReduce");
            Func<int, int,int>? reducer = (x,y) => {

                if (x < y)
                {
                    return y;
                }
                else{
                    return x;
                }
            };

            Func<int, int> mapper = value => value * 3 + 10;

            Func<int, bool> filter = value => value % 2 == 0;

            var input = Enumerable.Range(0,10).ToList();

            /// Running the test several times in order to ensure that it is not a random result.
            for(int i=0 ; i< 10_000 ; i++)
            {
                var resultado = MapReduce.Run(input, filter, mapper, reducer);

                var expected = 34;

                Assert.Equal(expected, resultado);
            }
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
            var resultadoEmParalelo = MapReduce.Run(lista, filtro, transform, null, null);
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
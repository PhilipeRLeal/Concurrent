using System.Diagnostics;

namespace Concurrent.UnitTest.Tests
{
    public class MapReduceTester
    {
        [Fact]
        public void Test()
        {
            Console.WriteLine("\nTeste MapReduce");
            var resultado = MapReduce.Run(Enumerable.Range(0,10), valor => valor *3 + 10, valor => valor % 2 == 0);
            var r = resultado.Select(x => (x-10)/3).Order().ToList();

            Assert.Equal(resultado.Order(), new List<int> { 10, 16, 22, 28, 34 });

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
            var resultadoEmParalelo = MapReduce.Run(lista, transform, filtro);
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

namespace Concurrent.UnitTest.Tests
{
    /// <summary>
    /// Testa se as divisões político administrativas foram semeadas corretamente no banco de dados.
    /// </summary>
    public class ParallelReducerTester
    {
        [Fact]
        public void Testar_ParallelReducer_via_Soma()
        {
            int N = 100;

            var lista = Enumerable.Range(0, N);

            Func<int,int,int> reducer = (x,y) => x + y;

            var result = lista.AsParallel().Reduce(reducer, 0);

            var expected = lista.Sum();

            Assert.Equal(result, expected);
        }

        /// <summary>
        /// To discuss result2
        /// </summary>
        [Fact]
        public void Testar_ParallelReducer_via_Subtracao()
        {
            int N = 100;

            var lista = Enumerable.Range(0, N).ToList();

            Func<int,int,int> reducer = (x,y) =>
            {
                var result = x - y;

                return result;
            };

            var result = lista.AsParallel().ReduceWithForImplementation(reducer, 0);

            var expected = lista.Aggregate(reducer);

            Assert.Equal(result, expected);

            var result2 = lista.AsParallel().Reduce(reducer, 0);

            // This result took me by surprise. Why is that?
            Assert.NotEqual(result, result2);
        }

        /// <summary>
        /// To discuss result2
        /// </summary>
        [Fact]
        public void Testar_ParallelReducer_via_Multiplicacao()
        {
            int N = 10;

            var lista = Enumerable.Range(1, N).ToList();

            var reducer = (int x, int y) => x * y;

            var result = lista.AsParallel().ReduceWithForImplementation(reducer, 1);

            var expected = lista.Aggregate(reducer);

            Assert.Equal(result, expected);

            var result2 = lista.AsParallel().Reduce(reducer, 0);

            // This result took me by surprise. Why is that?
            Assert.NotEqual(result, result2);
        }


        /// <summary>
        /// To discuss result2
        /// </summary>
        [Fact]
        public void Testar_ParallelReducer_via_Divisao()
        {
            int N = 10;

            var lista = Enumerable.Range(1, N).Select(x => (double)x).ToList();

            var reducer = (double x, double y) => x / y;

            var result = lista.AsParallel().ReduceWithForImplementation(reducer, 100000);

            double expected = 100000;

            for ( int i = 1 ; i < N ; i++ )
            {
                expected = expected / lista[i];
            }

            Assert.True(Math.Abs(result - expected ) < Math.Pow(10, -10));

            var result2 = lista.AsParallel().Reduce(reducer, 0);

            // This result took me by surprise. Why is that?
            Assert.NotEqual(result, result2);
        }
    }
}

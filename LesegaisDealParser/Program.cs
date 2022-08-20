using System;

namespace LesegaisDealParser
{
    class Program
    {
        static void Main(string[] args)
        {
            int entriesCountInPage = 2000;
            var parser = new Parser(entriesCountInPage);
            parser.Start();

            Console.ReadKey();
        }
    }
}

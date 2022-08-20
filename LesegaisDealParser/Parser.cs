using System;
using System.IO;
using System.Net;
using System.Text;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace LesegaisDealParser
{
    class Parser
    {
        private const string _adress = "https://www.lesegais.ru/open-area/graphql";

        private DataBase _db;

        /// <summary>
        ////в секундах
        /// </summary>
        private int _timeoutBetweenRequests;

        private int _entriesCountInPage;

        public Parser()
        {
            _db = new DataBase();
            _entriesCountInPage = 40;
        }

        public Parser(int entriesCountInPage)
        {
            _db = new DataBase();
            _entriesCountInPage = entriesCountInPage;
        }

        public Parser(int timeoutBetweenRequests, int entriesCountInPage)
        {
            _db = new DataBase();
            _timeoutBetweenRequests = SecondsToMilliseconds(_timeoutBetweenRequests);
            _entriesCountInPage = entriesCountInPage;
        }

        public Task Start()
        {
            Stopwatch timer = new Stopwatch();

            while (true)
            {
                int roundCount = 1;

                Console.WriteLine($"{DateTime.Now} - Начало {roundCount}-го обхода..");

                var startTime = DateTime.Now;

                for (int i = 1; ; i++)
                {
                    timer.Start();

                    var request = GetHttpWebRequestWithHeaders();
                    SetRequestDataForPage(request, i);
                    string response = GetResponseResult(request);

                    if (!IsResponseCorrect(response))
                    {
                        Console.WriteLine($"\n{DateTime.Now} - Произошла ошибка. Выполняется повторный запрос..");
                        i--;
                        continue;
                    }

                    var deals = GetDealsFromResponse(response);

                    if (deals.Count() == 0) break; //Конец парсинга

                    _db.InsertDealsIntoDB(deals);

                    timer.Stop();

                    Console.WriteLine($"\n{DateTime.Now} - Страница: {i}, Количество записей: {_entriesCountInPage}, " +
                        $"Время обработки: {timer.ElapsedMilliseconds / 1000} c.");
                    timer.Reset();

                    Thread.Sleep(_timeoutBetweenRequests);
                }

                var endTime = DateTime.Now;
                Console.WriteLine($"\n{DateTime.Now} - {roundCount}-й обход завершен.\n" +
                    $"Начало: {startTime}\nКонец: {endTime}\nВремя работы: {(endTime - startTime).Minutes} мин.\n" +
                    $"\nСледующий обхдод через 10 мин (в {DateTime.Now.AddMinutes(10).ToShortTimeString()})\n");

                Thread.Sleep(MinutesToMilliseconds(10));
            }
        }

        private bool IsResponseCorrect(string response)
        {
            return response != null && response.StartsWith("{\"data\":{\"searchReportWoodDeal\":{\"content\":");
        }

        private HttpWebRequest GetHttpWebRequestWithHeaders()
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(_adress);
            SetHeaders(request);
            return request;
        }

        private void SetHeaders(HttpWebRequest request)
        {
            request.Method = "Post";
            request.Accept = "*/*";
            request.Host = "www.lesegais.ru";
            request.ContentType = "application/json";
            request.Referer = "https://www.lesegais.ru/open-area/deal";
            request.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36";
            request.Headers.Add("Origin", "https://www.lesegais.ru");
            request.Headers.Add("sec-ch-ua", "\".Not / A)Brand\";v=\"99\", \"Google Chrome\";v=\"103\", \"Chromium\";v=\"103\"");
            request.Headers.Add("sec-ch-ua-mobile", "?0");
            request.Headers.Add("sec-ch-ua-platform", "\"Windows\"");
            request.Headers.Add("Sec-Fetch-Dest", "empty");
            request.Headers.Add("Sec-Fetch-Mode", "cors");
            request.Headers.Add("Sec-Fetch-Site", "same-origin");
        }

        private void SetRequestDataForPage(HttpWebRequest request, int page)
        {
            byte[] data = Encoding.UTF8.GetBytes(RequestDataForPage(page));
            request.ContentLength = data.Length;
            using (Stream stream = request.GetRequestStream())
            {
                stream.Write(data, 0, data.Length);
            }
        }

        private string RequestDataForPage(int page)
        {
            return @"{ "+
                       @"""operationName"": ""SearchReportWoodDeal""," +
                       @"""query"": ""query SearchReportWoodDeal" +
                       @"($size: Int!, $number: Int!, $filter: Filter, $orders: " +
                       @"[Order!]) {\n  searchReportWoodDeal(filter: $filter, pageable: " +
                       @"{number: $number, size: $size}, orders: $orders) {\n    content " +
                       @"{\n      sellerName\n      sellerInn\n      buyerName\n      buyerInn\n      " +
                       @"woodVolumeBuyer\n      woodVolumeSeller\n      dealDate\n      dealNumber\n      __typename\n    }" +
                       @"\n    __typename\n  }\n}\n""," +
                       @"""variables"": {" +
                           @"""filter"": null," +
                           @"""number"": " + (page - 1) + "," +
                           @"""orders"": [{""property"":""dealDate"",""direction"":""ASC""}]," + 
                           @"""size"": " + _entriesCountInPage +
                       @"}" +
                   @"}";
        }

        private string GetResponseResult(HttpWebRequest request)
        {
            string response = null;

            try
            {
                HttpWebResponse httpWebResponse = (HttpWebResponse)request.GetResponse();
                if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.OK)
                {
                    var responseStream = httpWebResponse.GetResponseStream();
                    using (var reader = new StreamReader(responseStream))
                    {
                        response = reader.ReadToEnd();
                    }
                    httpWebResponse.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n{DateTime.Now} - {ex.Message}");
            }

            return response;
        }

        private IEnumerable<JToken> GetDealsFromResponse(string response)
        {
            var jObject = JObject.Parse(response);
            return jObject["data"]["searchReportWoodDeal"]["content"];
        }

        private int MinutesToMilliseconds(int minutes) => minutes* 1000 * 60;

        private int SecondsToMilliseconds(int seconds) => seconds * 1000; 
    }
}

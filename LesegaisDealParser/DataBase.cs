using System;
using System.IO;
using Newtonsoft.Json.Linq;
using System.Configuration;
using System.Globalization;
using System.Data.SqlClient;
using System.Collections.Generic;

namespace LesegaisDealParser
{
    class DataBase
    {
        private string _connectionStr { get; }

        private SqlConnection _connection;

        public DataBase()
        {
            _connectionStr = ConfigurationManager.ConnectionStrings["DefaultConnection"]
                .ConnectionString.Replace("DBDirectory", Directory.GetParent(Environment.CurrentDirectory).Parent.FullName);
        }

        public void InsertDealsIntoDB(IEnumerable<JToken> woodDeals)
        {
            using (_connection = new SqlConnection(_connectionStr))
            {
                _connection.Open();
                foreach (var deal in woodDeals)
                {
                    string insertSqlExpression = GetInsertSqlExpressionAsString(deal);
                    SqlCommand insertCmd = new SqlCommand(insertSqlExpression, _connection);
                    try
                    {
                        insertCmd.ExecuteNonQuery();
                    }
                    catch (SqlException ex) when(ex.Message.Contains("Cannot insert duplicate key"))
                    {
                        UpdateEntryIfChanged(deal);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"\n{DateTime.Now} - {ex.Message}");
                    }
                }
            }
        }

        private void UpdateEntryIfChanged(JToken newDeal)
        {
            string selectSqlExpression = GetSelectByDealNumberExpressionAsString(newDeal);
            SqlCommand selectCmd = new SqlCommand(selectSqlExpression, _connection);

            string dataToUpdate = "";

            using (SqlDataReader reader = selectCmd.ExecuteReader())
            {
                reader.Read();

                var oldBuyerInn = (string)reader["Buyer_Inn"];
                var oldBuyerName = (string)reader["Buyer_Name"];
                var oldDealDate = reader["Deal_Date"];
                var oldSellerInn = (string)reader["Seller_Inn"];
                var oldSellerName = (string)reader["Seller_Name"];
                var oldWoodVolumeBuyer = reader["Wood_Volume_Buyer"].ToString();
                var oldWoodVolumeSeller = reader["Wood_Volume_Seller"].ToString();

                string newDealDate = newDeal["dealDate"].ToString();

                if (!string.IsNullOrEmpty(newDealDate) && ((DateTime)oldDealDate) <= DateTime.Parse(newDealDate))
                {
                    if (HasNewData(newDeal["buyerInn"].ToString(), oldBuyerInn))
                        dataToUpdate += $"Buyer_Inn = '{newDeal["buyerInn"]}'";

                    if ((DateTime)oldDealDate < DateTime.Parse(newDealDate))
                        dataToUpdate += $"{InsertCommaIfNeeded(dataToUpdate)}Deal_Date = '{newDeal["dealDate"]}'";

                    if (HasNewData(newDeal["buyerName"].ToString().ToLower(), oldBuyerName.ToLower()))
                        dataToUpdate += $"{InsertCommaIfNeeded(dataToUpdate)}" +
                            $"Buyer_Name = N'{newDeal["buyerName"].ToString().Replace("'", "''")}'";

                    if (HasNewData(newDeal["sellerInn"].ToString(), oldSellerInn))
                        dataToUpdate += $"{InsertCommaIfNeeded(dataToUpdate)}Seller_Inn = '{newDeal["sellerInn"]}'";

                    if (HasNewData(newDeal["sellerName"].ToString().ToLower(), oldSellerName.ToLower()))
                        dataToUpdate += $"{InsertCommaIfNeeded(dataToUpdate)}" +
                            $"Seller_Name = N'{newDeal["sellerName"].ToString().Replace("'", "''")}'";

                    if (HasNewData(newDeal["woodVolumeBuyer"].ToString(), oldWoodVolumeBuyer))
                        dataToUpdate += $"{InsertCommaIfNeeded(dataToUpdate)}Wood_Volume_Buyer = " + 
                            string.Format(CultureInfo.InvariantCulture, "{0}", newDeal["woodVolumeBuyer"]);

                    if (HasNewData(newDeal["woodVolumeSeller"].ToString(), oldWoodVolumeSeller))
                        dataToUpdate += $"{InsertCommaIfNeeded(dataToUpdate)}Wood_Volume_Seller = " + 
                            string.Format(CultureInfo.InvariantCulture, "{0}", newDeal["woodVolumeSeller"]);
                }
            }

            if (!string.IsNullOrEmpty(dataToUpdate))
            {
                string updateSqlExpression = GetUpdateExpressionAsString(dataToUpdate, newDeal["dealNumber"].ToString());
                SqlCommand updateCmd = new SqlCommand(updateSqlExpression, _connection);
                try
                {
                    updateCmd.ExecuteNonQuery();
                    Console.WriteLine($"\n{DateTime.Now} - {newDeal["dealNumber"].ToString()} обновлен:\n{dataToUpdate}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\n{DateTime.Now} - {ex.Message}");
                }
            }
        }

        private string InsertCommaIfNeeded(string dataToUpdate)
        {
            return string.IsNullOrEmpty(dataToUpdate) ? string.Empty : ", ";
        }
           
        private bool HasNewData(string newDealData, string oldDealData)
        {
            return !string.IsNullOrEmpty(newDealData) && oldDealData != newDealData;
        }

        private string GetInsertSqlExpressionAsString(JToken woodDeal)
        {
            // N - без него кириллица запишется в бд как вопрос-е знаки
            // CultureInfo.InvariantCulture - без него плавающая точка сохранится как запятая, из-за чего запрос в бд будет неправильным
            // реплейсом экранируем одинарные кавычки
            return "INSERT INTO Wood_Deals (Buyer_Inn, Buyer_Name, Deal_Date, Deal_Number, " +
                   "Seller_Inn, Seller_Name, Wood_Volume_Buyer, Wood_Volume_Seller)" +
                   $"VALUES('{woodDeal["buyerInn"]}', N'{woodDeal["buyerName"].ToString().Replace("'", "''")}', " +
                   $"'{woodDeal["dealDate"]}', '{woodDeal["dealNumber"]}', " +
                   $"'{woodDeal["sellerInn"]}', N'{woodDeal["sellerName"].ToString().Replace("'", "''")}', " +
                   string.Format(CultureInfo.InvariantCulture, "{0}, {1})",woodDeal["woodVolumeBuyer"], woodDeal["woodVolumeSeller"]);
        }


        private string GetSelectByDealNumberExpressionAsString(JToken woodDeal)
        {
            return $"SELECT * FROM Wood_Deals WHERE Deal_Number = '{woodDeal["dealNumber"]}'";
        }

        
        private string GetUpdateExpressionAsString(string dataToUpdate, string dealNumber)
        {
            return $"UPDATE Wood_Deals SET {dataToUpdate} WHERE Deal_Number = '{dealNumber}'";
        }
    }
}

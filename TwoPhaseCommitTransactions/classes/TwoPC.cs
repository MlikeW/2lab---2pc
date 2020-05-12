using System;
using NUnit.Framework;

namespace TwoPhaseCommitTransactions.classes
{
    [TestFixture]
    public class TwoPC
    {
        private const string FlyTransactionName = "FlyTransaction";
        private const string HotelTransactionName = "HotelTransaction";

        [Test]
        public void TestInsertIntoFly()
        {
            var flyDb = DbUtilities.GetAndOpenConnection("FlyBooking");
            flyDb.DropTableIfExist("FlyTable");
            flyDb.CreateFlyTable();
            flyDb.InsertIntoTable("FlyTable", $"('Kyiv', 'London', {"2020-10-10".ToDate()}, 50, 1)");
            flyDb.InsertIntoTable("FlyTable", $"('Kyiv', 'London', {"2020-10-12".ToDate()}, 50, 1)");
            flyDb.InsertIntoTable("FlyTable", $"('London', 'Kyiv', {"2020-10-15".ToDate()}, 50, 2)");

            flyDb.Close();
        }

        [Test]
        public void TestInsertIntoHotel()
        {
            var hotelDb = DbUtilities.GetAndOpenConnection("HotelBooking");
            hotelDb.DropTableIfExist("LondonCoolHotel");
            hotelDb.CreateHotelTable();
            hotelDb.InsertIntoTable("LondonCoolHotel",
                $"(121, 150, {"2020-10-12".ToDate()}, {"2020-10-15".ToDate()}, 1)");
            hotelDb.InsertIntoTable("LondonCoolHotel",
                $"(122, 150, {"2020-10-10".ToDate()}, {"2020-10-15".ToDate()}, 0)");

            hotelDb.Close();
        }

        [Test]
        public void TestInsertIntoCard()
        {
            var cardDb = DbUtilities.GetAndOpenConnection("Card");
            cardDb.DropTableIfExist("Account");
            cardDb.CreateAccountTable();
            cardDb.InsertIntoTable("Account", "('mybank', 30)");

            cardDb.Close();
        }

        [Test]
        public void TestAlmostFinalOk()
        {
            var flyDb = DbUtilities.GetAndOpenConnection("FlyBooking");
            var hotelDb = DbUtilities.GetAndOpenConnection("HotelBooking");
            var cardDb = DbUtilities.GetAndOpenConnection("Card");

            var transHotel = hotelDb.BeginTransaction();
            var transFly = flyDb.BeginTransaction();
            var transCard = cardDb.BeginTransaction();
            try
            {
                var flyCondition1 = $"fromcity = 'Kyiv' AND tocity = 'London' AND dateticket = {"2020-10-12".ToDate()}";
                var hotelCondition = $"roomfrom = {"2020-10-12".ToDate()} AND roomto = {"2020-10-15".ToDate()}";
                var flyCondition2 = $"fromcity = 'London' AND tocity = 'Kyiv' AND dateticket = {"2020-10-15".ToDate()}";

                var priceFly1 = flyDb.SelectFromTable<int>("FlyTable", flyCondition1, "price");
                flyDb.UpdateTable("FlyTable", "availabillity".Minus(), $"{flyCondition1} AND price = {priceFly1}");
                var priceHotel = hotelDb.SelectFromTable<int>("LondonCoolHotel", hotelCondition, "roomprice");
                hotelDb.UpdateTable("LondonCoolHotel", "availabillity".Minus(), $"{hotelCondition} AND roomprice = {priceHotel}");
                var priceFly2 = flyDb.SelectFromTable<int>("FlyTable", flyCondition2, "price");
                flyDb.UpdateTable("FlyTable", "availabillity".Minus(), $"{flyCondition2} AND price = {priceFly2}");

                cardDb.UpdateTable("Account", "amount".Minus(priceFly1 + priceFly2 + priceHotel), $"bank = 'mybank'");

                flyDb.PrepareTransaction(FlyTransactionName);
                hotelDb.PrepareTransaction(HotelTransactionName);

                flyDb.CommitPrepared(FlyTransactionName);
                hotelDb.CommitPrepared(HotelTransactionName);
            }
            catch (Exception ex)
            {
                transHotel.Rollback();
                transFly.Rollback();
                transCard.Rollback();
                Console.WriteLine($"\nTransaction Failed: \n{ex.Message}");
            }

            if (flyDb.SelectAllFromTable<string>("pg_prepared_xacts", "gid") != null)
            {
                throw new Exception("Prepared Transaction Table not empty");
            }

            flyDb.Close();
            hotelDb.Close();
            cardDb.Close();
            Console.WriteLine("\nTransaction Successful");
        }
    }
}
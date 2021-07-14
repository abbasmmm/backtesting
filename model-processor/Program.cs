using Cassandra;
using Dapper;
using IronPython.Hosting;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Scripting;
using Microsoft.Scripting.Hosting;
using MySql.Data.MySqlClient;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace data_loader
{
    class Program
    {
        static Dictionary<string, ModelExecutor> modelExecutors = new Dictionary<string, ModelExecutor>();
        static int numOfThreads = 0;
        private static string nodeName;
        static HubConnection hubCon = null;
        static bool isModelRunComplete = false;
        static ConcurrentQueue<TradeInput> tradeData = new ConcurrentQueue<TradeInput>();
        static object baton = new object();
        static int totalRecsProcessed;
        static void Main(string[] args)
        {
            hubCon = new HubConnectionBuilder()
                    .WithUrl("http://localhost:5000/coordination-hub")
                    .Build();
            hubCon.HandshakeTimeout = TimeSpan.FromSeconds(60);
            hubCon.ServerTimeout = TimeSpan.FromSeconds(60);

            numOfThreads = int.Parse(args[3]);
            nodeName = args[1];
            hubCon.Closed += async (error) =>
            {
                await Task.Delay(new Random().Next(0, 5) * 1000);
                await hubCon.StartAsync();
            };
            hubCon.On("RunModel", RunModel);
            hubCon.On<List<TradeInput>>("LoadTrades", LoadTrades);
            hubCon.On("ModelRunCompleted", () => { isModelRunComplete = true; });
            hubCon.StartAsync().Wait();
            hubCon.SendAsync("Connect", nodeName, numOfThreads).Wait();

            Stopwatch watch = new Stopwatch();

            var cluster = Cluster.Builder()
                                 .AddContactPoints("localhost")
                                 .Build();

            // Connect to the nodes using a keyspace
            session = cluster.Connect("backtesting");
            
            using (var connection = new MySqlConnection("Server=localhost;Database=backtesting;Uid=root;Pwd=different#1;"))
            {

                models = connection.Query<Model>("SELECT * FROM backtesting.model").ToList();
                foreach (var model in models)
                {
                    modelExecutors[model.Name] = new ModelExecutor(model.Code, model.Imports);
                }

                var tradeData = connection.Query<TradeInput>("SELECT * FROM backtesting.trade_input_details limit 3").ToList();
                var dataReader = connection.ExecuteReader("select * from backtesting.SigmaVolatility");
                var dataTable = new DataTable();
                dataTable.Load(dataReader);
                sigma = dataTable.AsEnumerable().Select(x => x.ItemArray.Select(y => float.Parse(y.ToString())).ToList()).ToList();
                columns = new List<string>();
                foreach (DataColumn column in dataTable.Columns)
                {
                    columns.Add(column.ColumnName);
                }

                modelResultInsert = session.Prepare(@$"INSERT INTO model_results(tradeid, version, {string.Join(',', columns)}) VALUES (?,?,{string.Join(',', columns.Select(x => "?"))})");
                modelResultMetaDataInsert = session.Prepare($"insert into model_result_metadata(tradeid,modelname,latestversion,lastprocessed) values(?,?,?,?)");
                watch.Restart();
                dataReader = connection.ExecuteReader("select * from backtesting.RiskLessShortRate");
                dataTable = new DataTable();
                dataTable.Load(dataReader);
                risk = dataTable.AsEnumerable().Select(x => x.ItemArray.Select(y => float.Parse(y.ToString())).ToList()).ToList();
                watch.Stop();

                recCount = sigma.Count;
                colCount = columns.Count;
            }

            Console.ReadLine();
            Console.ReadLine();
            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        static List<Task> tasks = new List<Task>();
        private static List<Model> models;
        private static List<List<float>> sigma;
        private static List<string> columns;
        private static PreparedStatement modelResultInsert;
        private static PreparedStatement modelResultMetaDataInsert;
        private static List<List<float>> risk;
        private static int recCount;
        private static int colCount;
        private static ISession session;

        public static void RunModel()
        {
            isModelRunComplete = false;
            totalRecsProcessed = 0;
            tasks.Clear();

            Log($"Node : {nodeName} is ready to process the model, waiting for trade data..");
            for (int i = 0; i < numOfThreads; i++)
            {
                var threadId = i + 1;
                tasks.Add(Task.Run(() => WorkerProcess(threadId)));
            }
        }

        private static void WorkerProcess(int threadId)
        {
            Stopwatch watch = new Stopwatch();

            while (!isModelRunComplete || tradeData.Any())
            {
                if (tradeData.TryDequeue(out TradeInput trade))
                {
                    int version = GetVersion(trade.TradeId);
                    watch.Restart();
                    var modelExecutor = modelExecutors[trade.ModelName];
                    BatchStatement batch = new BatchStatement().SetBatchType(BatchType.Logged);
                    for (int i = 0; i < recCount; i++)
                    {
                        var curRow = new List<object>() { trade.TradeId, version, (int)risk[i][0] };
                        for (int j = 1; j < colCount; j++)
                        {
                            var r = risk[i][j];
                            var s = sigma[i][j];
                            var s0 = trade.InitialIndexLevel;
                            var x = trade.StrikePrice;
                            var t = trade.TimeToMaturity;

                            var res = modelExecutor.callPrice(s0, x, r, s, t);
                            curRow.Add(res);
                        }
                        var st = modelResultInsert.Bind(curRow.ToArray());
                        batch.Add(st);
                        if (i % 100 == 0)
                        {
                            session.ExecuteAsync(batch);
                            batch = new BatchStatement().SetBatchType(BatchType.Logged);
                        }
                    }

                    session.ExecuteAsync(batch);
                    session.ExecuteAsync(modelResultMetaDataInsert.Bind(new object[] { trade.TradeId, trade.ModelName, version, new DateTimeOffset(DateTime.Now) }));

                    watch.Stop();
                    totalRecsProcessed += 1;
                    Log(trade.TradeId + "(V-" + version + ") processed !!, time: " + watch.ElapsedMilliseconds + " ms");
                }
                else
                {
                    Task.Delay(500).Wait();
                }
            }

            Log($"Thread {threadId} Finished processing, killing the current thread.");
        }

        private static int GetVersion(string tradeId)
        {
            var res = session.Execute($"select latestversion from model_result_metadata where tradeid='{tradeId}'");

            foreach (var row in res)
            {
                return row.GetValue<int>("latestversion") + 1;
            }

            return 1;
        }

        private static void Log(string message)
        {
            hubCon.SendAsync("Log", message, tradeData.Count, totalRecsProcessed);
            Console.WriteLine(message);
        }

        private static void LoadTrades(List<TradeInput> tradeInputs)
        {
            foreach (var trade in tradeInputs)
            {
                tradeData.Enqueue(trade);
            }

            Log($"{tradeInputs.Count} new trades received from master process.");
        }
    }

    public class Model
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Code { get; set; }
        public string Imports { get; set; }
    }

    public class TradeInput
    {
        public string TradeId { get; set; }
        public DateTime ExcersieDate { get; set; }
        public DateTime BusinessDate { get; set; }
        public DateTime MaturityDate { get; set; }
        public int TimeToMaturity { get; set; }
        public DateTime SimulationDate { get; set; }
        public string ModelType { get; set; }
        public string ModelName { get; set; }
        public double StrikePrice { get; set; }
        public string CallPut { get; set; }
        public string BuySell { get; set; }
        public int Spread { get; set; }
        public float InitialIndexLevel { get; set; }
    }
}

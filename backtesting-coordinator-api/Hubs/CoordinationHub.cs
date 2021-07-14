using Dapper;
using Microsoft.AspNet.SignalR.Hubs;
using Microsoft.AspNetCore.SignalR;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace backtesting_coordinator_api.Hubs
{
    [HubName("coordination-hub")]
    public class CoordinationHub : Hub<ICoordinationHub>
    {
        static Dictionary<string, Processor> processes = new Dictionary<string, Processor>();
        static HashSet<string> uiConnections = new HashSet<string>();
        static ConcurrentQueue<TradeInput> tradeData = new ConcurrentQueue<TradeInput>();
        static bool isModelRunInProgress = false;
        static int totalThreads = 0;
        static MySqlConnection connection = new MySqlConnection("Server=localhost;Database=backtesting;Uid=root;Pwd=different#1;");
        private int start = 0, totalRecordsLoaded = 0;
        private static bool endOfQueue;

        public void Connect(string name, int threads)
        {
            processes[Context.ConnectionId] = new Processor { NumOfThreads = threads, Name = name };
            totalThreads += threads;

            if (uiConnections.Any())
                this.Clients.Clients(uiConnections).UpdateProcessors(processes.Values.ToList());
        }

        public void ConnectUi()
        {
            uiConnections.Add(Context.ConnectionId);
            this.Clients.Client(Context.ConnectionId).UpdateProcessors(processes.Values.ToList());
        }

        public void RunModel()
        {
            if (!isModelRunInProgress)
            {
                SendLogToUi("Coord", "Run Model command has been received from UI", tradeData.Count, 0);
                isModelRunInProgress = true;
                LoadTradeData();
                Clients.Clients(processes.Keys.ToArray()).RunModel();
                StartDataLoaderThread();
            }
        }

        public void Log(string message, int queueDepth, int recsProcessed)
        {
            var curProc = processes[Context.ConnectionId];
            SendLogToUi(curProc.Name, message, queueDepth, recsProcessed);
            var minExpected = curProc.NumOfThreads * 10;
            if (queueDepth < minExpected)
            {
                var lst = new List<TradeInput>();
                for (int i = 0; i < minExpected; i++)
                {
                    if (tradeData.TryDequeue(out var trade))
                    {
                        lst.Add(trade);
                    }
                    else if (endOfQueue)
                    {
                        break;
                    }
                }

                if (lst.Any())
                {
                    Clients.Client(Context.ConnectionId).LoadTrades(lst);
                    SendLogToUi("Coord", $"Sending {lst.Count} trades to node : {curProc.Name}", tradeData.Count, 0);
                }
                else if (queueDepth == 0 && endOfQueue && !curProc.IsCompleted)
                {
                    Clients.Client(Context.ConnectionId).ModelRunCompleted();
                    curProc.IsCompleted = true;
                    SendLogToUi(curProc.Name, $"Process completed!! : {curProc.Name}", tradeData.Count, recsProcessed, true);


                    if (processes.Values.All(x => x.IsCompleted))
                    {
                        isModelRunInProgress = false;
                        SendLogToUi("Coord", $"Process completed!!", tradeData.Count, 0, true);
                    }
                }
                else
                {
                    SendLogToUi(curProc.Name, $"Test NQ {queueDepth}, CQ {tradeData.Count}, EOQ {endOfQueue}, IsCom {curProc.IsCompleted}", tradeData.Count, recsProcessed, true);
                }
            }
        }

        private Task SendLogToUi(string name, string message, int queueDepth, int recsProcessed, bool isCompleted = false)
        {
            return Clients.Clients(uiConnections).Log(name, message, queueDepth, recsProcessed, isCompleted);
        }

        private void StartDataLoaderThread()
        {
            Task.Run(() =>
            {
                while (true && !endOfQueue)
                {
                    if (tradeData.Count < totalThreads * 20)
                        LoadTradeData();
                }
            });
        }

        public override async Task OnDisconnectedAsync(Exception exception)
        {
            totalThreads -= processes[Context.ConnectionId].NumOfThreads;
            if (processes.ContainsKey(Context.ConnectionId))
            {
                processes.Remove(Context.ConnectionId);
                await this.Clients.Clients(uiConnections).UpdateProcessors(processes.Values.ToList());
            }
            else
            {
                uiConnections.Remove(Context.ConnectionId);
            }
            await base.OnDisconnectedAsync(exception);
        }

        private void LoadTradeData()
        {
            var res = connection.Query<TradeInput>($"SELECT * FROM backtesting.trade_input_details limit {start},{totalThreads * 20};").ToList();
            var logtxt = $"Loaded records from {start + 1} to {start + (totalThreads * 20) + 1} into master queue.";
            start = start + (totalThreads * 20);
            foreach (var re in res)
            {
                tradeData.Enqueue(re);
            }

            if (res.Any())
                SendLogToUi("Coord", logtxt, tradeData.Count, 0);

            if (res.Count < totalThreads * 20)
            {
                endOfQueue = true;
                SendLogToUi("Coord", "There are no more records to read for given criteria.", tradeData.Count, 0);
            }
        }
    }

    public class Processor
    {
        public int NumOfThreads { get; set; }
        public string Name { get; set; }
        public int QueueDepth { get; set; }
        public int RecordsProcessed { get; set; }
        public bool IsCompleted { get; set; }
    }

    public interface ICoordinationHub
    {
        Task LoadTrades(List<TradeInput> lst);
        Task Log(string name, string message, int queueDepth, int recsProcessed, bool isCompleted);
        Task ModelRunCompleted();
        Task RunModel();
        Task UpdateProcessors(List<Processor> list);
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

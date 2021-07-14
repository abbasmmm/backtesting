using Cassandra;
using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace backtesting_coordinator_api.Controllers
{

    [Route("api")]
    [ApiController]
    public class ResultController : ControllerBase
    {
        private static Queue<TradeInput> trades = new Queue<TradeInput>();
        private Cluster cluster;
        private readonly string[] columns = new string[] { "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20", "c21", "c22", "c23", "c24", "c25", "c26", "c27", "c28", "c29", "c30", "c31", "c32", "c33", "c34", "c35", "c36", "c37", "c38", "c39", "c40", "c41", "c42", "c43", "c44", "c45", "c46", "c47", "c48", "c49", "c50", "c51", "c52", "c53", "c54", "c55", "c56", "c57", "c58", "c59", "c60", "c61", "c62", "c63", "c64", "c65", "c66", "c67", "c68", "c69", "c70", "c71", "c72", "c73", "c74" };

        public ResultController()
        {
            cluster = Cluster.Builder()
                                 .AddContactPoints("localhost")
                                 .Build();
        }

        [HttpGet]
        [Route("get-models")]
        public List<Model> GetModels(string tradeId)
        {
            using (var connection = new MySqlConnection("Server=localhost;Database=backtesting;Uid=root;Pwd=different#1;"))
            {
                return connection.Query<Model>("SELECT * FROM backtesting.model").ToList();                
            }
        }


        [HttpPut]
        [Route("update-model")]
        public void UpdateModel(Model model)
        {
            using (var connection = new MySqlConnection("Server=localhost;Database=backtesting;Uid=root;Pwd=different#1;"))
            {
                connection.ExecuteScalar($"update backtesting.model set name='{model.Name}', imports='{model.Imports}', code='{model.Code}' where id = {model.Id}");
            }
        }

        [HttpGet]
        [Route("csv/{tradeId}")]
        public FileContentResult GetTradeResultCsv(string tradeId)
        {
            return GetCsvResult(tradeId);
        }

        [HttpGet]
        [Route("csv/{tradeId}/version/{version}")]
        public FileContentResult GetTradeResultCsv(string tradeId, int version)
        {
            return GetCsvResult(tradeId, version);
        }

        [HttpGet]
        [Route("csv/{tradeId}/compare-versions-v2/{versions}")]
        public FileContentResult GetTradeResultCsvComparison(string tradeId, string versions)
        {
            var dic = GetTradeResultJsonComparisonV2(tradeId, versions);
            var vers = versions.Split(',').Select(x => int.Parse(x)).ToList();

            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Scenario,{string.Join(',', vers.Select(x => "v" + x))}");
            foreach (var val in dic)
            {
                sb.AppendLine($"{val.Key},{string.Join(',', vers.Select(x => val.Value["v" + x]))}");
            }

            return GetFile($"{tradeId}-v_{versions}.csv", sb.ToString());
        }

        private FileContentResult GetCsvResult(string tradeId, int version = 0)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append($"tradeid,version,scenario,{string.Join(',', columns)}");
            sb.Append(Environment.NewLine);
            var rows = FetchTradeResult(tradeId, ref version);
            foreach (var row in rows)
            {
                sb.Append($"{tradeId},{version},");
                sb.Append(row.GetValue<int>("scenario"));
                foreach (var col in columns)
                {
                    sb.Append("," + row.GetValue<double>(col));
                }
                sb.Append(Environment.NewLine);
            }

            var filename = tradeId + "-v_" + version + ".csv";
            var content = sb.ToString();
            FileContentResult result = GetFile(filename, content);
            return result;
        }

        private static FileContentResult GetFile(string filename, string content)
        {
            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream);
            writer.Write(content);
            writer.Flush();
            stream.Position = 0;

            var result = new FileContentResult(stream.ToArray(), "application/octet-stream");
            result.FileDownloadName = filename;
            return result;
        }

        [HttpGet]
        [Route("json/{tradeId}")]
        public ModelResult GetTradeResultJson(string tradeId)
        {
            return GetJsonResult(tradeId);
        }

        [HttpGet]
        [Route("json/{tradeId}/version/{version}")]
        public ModelResult GetTradeResultJson(string tradeId, int version)
        {
            return GetJsonResult(tradeId, version);
        }

        [HttpGet]
        [Route("json/{tradeId}/compare-versions/{versions}")]
        public ModelComparisonResult GetTradeResultJsonComparison(string tradeId, string versions)
        {
            return GetJsonResultComparison(tradeId, versions);
        }

        private ModelComparisonResult GetJsonResultComparison(string tradeId, string version)
        {
            var vers = version.Split(',').Select(x => int.Parse(x)).ToList();
            Dictionary<int, List<Row>> data = new Dictionary<int, List<Row>>();

            foreach (var ver in vers)
            {
                var verN = ver;
                data[ver] = FetchTradeResult(tradeId, ref verN);
            }

            var res = new ModelComparisonResult() { TradeId = tradeId, ScenarioData = new Dictionary<string, Dictionary<string, double>>() };

            for (int i = 0; i < data[vers[0]].Count; i++)
            {
                var row = data[vers[0]][i];
                var scenario = row.GetValue<int>("scenario");
                foreach (var col in columns)
                {
                    res.ScenarioData[$"S{scenario}_{col}"] = vers.ToDictionary(x => "v" + x, x => data[vers[0]][i].GetValue<double>(col));
                }
            }

            return res;
        }


        [HttpGet]
        [Route("json/{tradeId}/compare-versions-v2/{versions}")]
        public Dictionary<string, Dictionary<string, double>> GetTradeResultJsonComparisonV2(string tradeId, string versions)
        {
            return GetJsonResultComparisonV2(tradeId, versions);
        }

        private Dictionary<string, Dictionary<string, double>> GetJsonResultComparisonV2(string tradeId, string version)
        {
            var vers = version.Split(',').Select(x => int.Parse(x)).ToList();
            Dictionary<int, List<Row>> data = new Dictionary<int, List<Row>>();

            foreach (var ver in vers)
            {
                var verN = ver;
                data[ver] = FetchTradeResult(tradeId, ref verN);
            }

            var res = new Dictionary<string, Dictionary<string, double>>();

            for (int i = 0; i < data[vers[0]].Count; i++)
            {
                var row = data[vers[0]][i];
                var scenario = row.GetValue<int>("scenario");
                foreach (var col in columns)
                {
                    res[$"S{scenario}_{col}"] = vers.ToDictionary(x => "v" + x, x => data[x][i].GetValue<double>(col));
                }
            }

            return res;
        }

        private ModelResult GetJsonResult(string tradeId, int version = 0)
        {
            var rows = FetchTradeResult(tradeId, ref version);
            var res = new ModelResult() { Version = version, TradeId = tradeId, ScenarioData = new Dictionary<string, double>() };

            foreach (var row in rows)
            {
                var scenario = row.GetValue<int>("scenario");
                foreach (var col in columns)
                {
                    res.ScenarioData[$"S{scenario}_{col}"] = row.GetValue<double>(col);
                }
            }

            return res;
        }

        private List<Row> FetchTradeResult(string tradeId, ref int version)
        {
            using (var session = cluster.Connect("backtesting"))
            {
                if (version == 0)
                {
                    var res = session.Execute($"select latestversion from model_result_metadata where tradeid='{tradeId}'");
                    foreach (var row in res)
                    {
                        version = row.GetValue<int>("latestversion");
                        break;
                    }
                }
                var statement = new SimpleStatement($"select TradeId, Version, Scenario,{string.Join(',', columns)} from model_results where tradeid='{tradeId}' and version={version}");
                return session.Execute(statement).ToList();
            }
        }
    }

    public class ModelResult
    {
        public string TradeId { get; set; }
        public int Version { get; set; }
        public Dictionary<string, double> ScenarioData { get; set; }
    }

    public class ModelComparisonResult
    {
        public string TradeId { get; set; }
        public Dictionary<string, Dictionary<string, double>> ScenarioData { get; set; }
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

    public class Model
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Code { get; set; }
        public string Imports { get; set; }
    }
}

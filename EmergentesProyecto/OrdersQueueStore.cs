using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;

namespace EmergentesProyecto {
    // Espejo persistente de las ordenes que llegan del backend.
    // No reemplaza a _orderQueue (que sigue viviendo en memoria para
    // la operacion normal): esto es una copia de resguardo en disco,
    // para poder recuperar ordenes pendientes si el proceso de Worker se cae o reinicia antes de haberlas despachado.
    public class OrdersQueueStore {
        private const int StatusPending = 0;
        private const int StatusDone = 1;

        private readonly string _connectionString;

        public OrdersQueueStore(string dbPath = "orders_queue.db") {
            _connectionString = $"Data Source={dbPath}";
            Init();
        }

        private void Init(){
            using var conn = new SqliteConnection(_connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                PRAGMA journal_mode=WAL;
                PRAGMA busy_timeout=5000;
                CREATE TABLE IF NOT EXISTS orders_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL UNIQUE,
                    order_json TEXT NOT NULL,
                    status INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_orders_status ON orders_queue(status);
            ";
            cmd.ExecuteNonQuery();
        }

        // Guarda la orden si todavia no existe (idempotente por order_id).
        // Se llama apenas llega la orden del backend, antes de intentar despacharla a ningun rover.
        public void Mirror(string orderId, string orderJson){
            using var conn = new SqliteConnection(_connectionString);
            conn.Open(); 
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                INSERT OR IGNORE INTO orders_queue (order_id, order_json, status, created_at)
                VALUES ($orderId, $orderJson, $status, $createdAt);";
            cmd.Parameters.AddWithValue("$orderId", orderId);
            cmd.Parameters.AddWithValue("$orderJson", orderJson);
            cmd.Parameters.AddWithValue("$status", StatusPending);
            cmd.Parameters.AddWithValue("$createdAt", DateTime.UtcNow.ToString("o"));
            cmd.ExecuteNonQuery();
        }

        // Se llama cuando la orden ya fue publicada con exito al rover.
        public void MarkDone(string orderId) {
            using var conn = new SqliteConnection(_connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE orders_queue SET status = $status WHERE order_id = $orderId;";
            cmd.Parameters.AddWithValue("$status", StatusDone);
            cmd.Parameters.AddWithValue("$orderId", orderId);
            cmd.ExecuteNonQuery();
        }

        // Se llama una sola vez al arrancar el Worker: recupera cualquier orden que haya quedado pendiente de un crash o reinicio anterior,
        // para volver a encolarla en memoria y que siga el flujo normal.
        public List<JsonNode> LoadPending() {
            var result = new List<JsonNode>();
            using var conn = new SqliteConnection(_connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT order_json FROM orders_queue WHERE status = $status ORDER BY id ASC;";
            cmd.Parameters.AddWithValue("$status", StatusPending);
            using var reader = cmd.ExecuteReader();
            while (reader.Read()) {
                var json = reader.GetString(0);
                var node = JsonNode.Parse(json);
                if (node != null) result.Add(node);
            }
            return result;
        }
    }
}

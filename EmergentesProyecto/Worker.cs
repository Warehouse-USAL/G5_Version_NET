using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using MQTTnet;


namespace EmergentesProyecto
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private IMqttClient _client;
        

        // Estructuras de Datos (Equivalentes seguras para multihilo)
        private readonly ConcurrentDictionary<string, RoverState> _rovers = new();
        private readonly ConcurrentQueue<JsonNode> _orderQueue = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastBackendTelemetry = new();

        // Constantes
        private const string BrokerIp = "127.0.0.1"; // Cambiar por la IP de tu RabbitMQ
        private const int TimeoutRoverSec = 30;
        private const int BackendTelemetryInterval = 60;

        // Tópicos
        private const string TopicFromBackend = "backend/orders";
        private const string TopicToBackend = "central/backend_updates";
        private const string TopicRoverTelemetry = "vehicles/+/telemetry";
        private const string TopicRoverResponse = "vehicles/+/vehicle_response";

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
            var factory = new MqttClientFactory();
            _client = factory.CreateMqttClient();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var options = new MqttClientOptionsBuilder()
                .WithTcpServer(BrokerIp, 1883)
                .WithCredentials("admin_admin", "admin") // Usa admin_iot si ya lo creaste en RabbitMQ
                .WithClientId($"CentralService_{Guid.NewGuid()}")
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V311)
                .Build();

            _client.ConnectedAsync += async e =>
            {
                _logger.LogInformation("Central conectada exitosamente al Broker MQTT");

                var factory = new MqttClientFactory();
                var subscribeOptions = factory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(f => f.WithTopic(TopicFromBackend).WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce))
                    .WithTopicFilter(f => f.WithTopic(TopicRoverTelemetry).WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce))
                    .WithTopicFilter(f => f.WithTopic(TopicRoverResponse).WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce))
                    .Build();

                await _client.SubscribeAsync(subscribeOptions, stoppingToken);
            };

            _client.ApplicationMessageReceivedAsync += HandleMessageReceivedAsync;

            try
            {
                await _client.ConnectAsync(options, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogCritical("Error fatal al conectar: {Message}", ex.Message);
                return;
            }

            // Lanza el Watchdog en paralelo
            _ = CheckAliveRoversAsync(stoppingToken);

            // Mantiene el servicio corriendo
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        private async Task HandleMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
        {
            var topic = e.ApplicationMessage.Topic;
            var payloadString = e.ApplicationMessage.ConvertPayloadToString();

            try
            {
                var jsonNode = JsonNode.Parse(payloadString);
                if (jsonNode == null) return;

                if (topic.Contains("telemetry"))
                {
                    await HandleTelemetryAsync(jsonNode);
                }
                else if (topic.Contains("vehicle_response"))
                {
                    await HandleRoverResponseAsync(topic, jsonNode);
                }
                else if (topic == TopicFromBackend)
                {
                    await HandleBackendOrdersAsync(jsonNode);
                }
            }
            catch (JsonException)
            {
                _logger.LogWarning("Mensaje mal formado recibido en {Topic}", topic);
            }
            catch (Exception ex)
            {
                _logger.LogError("Error procesando mensaje: {Message}", ex.Message);
            }
        }

        private async Task HandleTelemetryAsync(JsonNode data)
        {
            var vId = data["vehicle_id"]?.ToString();
            var status = data["status"]?.ToString() ?? "unknown";
            if (string.IsNullOrEmpty(vId)) return;

            var now = DateTime.UtcNow;

            _rovers.AddOrUpdate(vId,
                _ => new RoverState { Status = status, LastSeen = now, Failed = false },
                (_, state) =>
                {
                    state.Status = status;
                    state.LastSeen = now;
                    state.Failed = false;
                    return state;
                });

            // Filtrado de 60 segundos
            _lastBackendTelemetry.TryGetValue(vId, out var lastSent);
            if ((now - lastSent).TotalSeconds >= BackendTelemetryInterval)
            {
                _lastBackendTelemetry[vId] = now;

                double batteryVoltage = data["battery_voltage"]?.GetValue<double>() ?? 0;
                int batteryPercentage = (int)((batteryVoltage / 3.3) * 100);

                var backendTelemetryMsg = new
                {
                    message_type = "vehicle.telemetry",
                    vehicle_id = vId,
                    position = new { x = 0.0, y = 0.0 },
                    battery = batteryPercentage,
                    status = status,
                    timestamp = now.ToString("yyyy-MM-ddTHH:mm:ssZ")
                };

                await PublishAsync(TopicToBackend, JsonSerializer.Serialize(backendTelemetryMsg));
                _logger.LogInformation("🚀 Telemetría de {Vid} enviada al Backend.", vId);
            }

            await ProcessQueueAsync();
        }

        private async Task HandleBackendOrdersAsync(JsonNode data)
        {
            var msgType = data["message_type"]?.ToString();
            if (msgType == "order.dispatch")
            {
                _logger.LogInformation("Nueva orden {OrderId} encolada.", data["order_id"]?.ToString());
                _orderQueue.Enqueue(data);
                await ProcessQueueAsync();
            }
        }

        private async Task ProcessQueueAsync()
        {
            if (_orderQueue.IsEmpty) return;

            string targetRover = null;
            var now = DateTime.UtcNow;

            // Buscar un rover disponible (idle, visto hace menos de 30s y sin fallo)
            foreach (var kvp in _rovers)
            {
                if (kvp.Value.Status == "idle" && !kvp.Value.Failed && (now - kvp.Value.LastSeen).TotalSeconds < TimeoutRoverSec)
                {
                    targetRover = kvp.Key;
                    break;
                }
            }

            if (targetRover != null)
            {
                if (_orderQueue.TryDequeue(out var orderData))
                {
                    var orderId = orderData["order_id"]?.ToString();
                    _logger.LogInformation("🎯 Despachando orden {OrderId} al canal de órdenes de {TargetRover}", orderId, targetRover);

                    _rovers[targetRover].Status = "busy";

                    var topicOrder = $"vehicles/{targetRover}/vehicle_order";
                    await PublishAsync(topicOrder, orderData.ToJsonString());
                }
            }
        }

        private async Task HandleRoverResponseAsync(string topic, JsonNode payload)
        {
            var parts = topic.Split('/');
            var vId = parts.Length > 1 ? parts[1] : "UNKNOWN";

            _logger.LogInformation("📩 Respuesta recibida desde {Vid} (Canal response). Reenviando a backend_updates...", vId);

            // Agregamos el timestamp inyectándolo directamente en el nodo JSON
            payload["central_timestamp"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

            await PublishAsync(TopicToBackend, payload.ToJsonString());
        }

        private async Task CheckAliveRoversAsync(CancellationToken stoppingToken)
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));

            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                var now = DateTime.UtcNow;
                foreach (var kvp in _rovers)
                {
                    var vId = kvp.Key;
                    var info = kvp.Value;

                    if ((now - info.LastSeen).TotalSeconds > TimeoutRoverSec && !info.Failed)
                    {
                        info.Failed = true;
                        info.Status = "disconnected";

                        var errorMsg = new
                        {
                            message_type = "vehicle.error",
                            vehicle_id = vId,
                            error_code = "CONNECTION_LOST",
                            message = $"Sin señal desde hace {TimeoutRoverSec} segundos.",
                            timestamp = now.ToString("yyyy-MM-ddTHH:mm:ssZ")
                        };

                        await PublishAsync(TopicToBackend, JsonSerializer.Serialize(errorMsg));
                        _logger.LogError("🚨 [ALERTA] Rover {Vid} desconectado. Notificado al backend.", vId);
                    }
                }
            }
        }

        private async Task PublishAsync(string topic, string payload)
        {
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await _client.PublishAsync(message);
        }
    }
}

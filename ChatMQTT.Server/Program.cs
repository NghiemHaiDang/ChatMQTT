// ==============================================================
// BROKER SERVER
// ==============================================================
// MQTT Broker tự tạo sử dụng thư viện MQTTnet
// MQTT Broker hoạt động như một trung gian (middleman):
// - Nhận tin nhắn từ các Publisher (người gửi)
// - Chuyển tiếp tin nhắn đến các Subscriber (người nhận)
// - Quản lý kết nối của các client
// - Theo dõi các topic (chủ đề) và ai đang subscribe

using MQTTnet;              
using MQTTnet.Server;       
using System.Text;          
using System.Collections.Concurrent;
using System.Net;
using BuildingBlocks.Models.Servers;

namespace ChatMQTT.Broker
{
    class Program
    {
        #region Global variables

        // Dictionary lưu thông tin tất cả clients đã kết nối
        // Key: ClientId (string) - ID duy nhất của client
        // Value: ClientInfo object - chứa thông tin chi tiết về client
        // ConcurrentDictionary = thread-safe, nhiều thread có thể truy cập cùng lúc không bị lỗi
        private static readonly ConcurrentDictionary<string, ClientInfo> ConnectedClients = new();

        // Dictionary lưu danh sách subscribers cho mỗi topic
        // Key: Topic name (string) - tên chủ đề như "chat/general"
        // Value: ConcurrentBag<string> - danh sách ClientId đang subscribe topic này
        // ConcurrentBag = thread-safe collection, cho phép add/remove an toàn
        private static readonly ConcurrentDictionary<string, ConcurrentBag<string>> TopicSubscribers = new();

        // Tổng số tin nhắn đã xử lý
        private static int totalMessagesProcessed = 0;

        // Tổng số kết nối đã phục vụ (kể cả đã disconnect)
        private static int totalConnectionsServed = 0;

        #endregion Global variables

        #region Main Function

        static async Task Main(string[] args)
        {
            ShowBanner(); // Show Banner

            // Lấy thông tin cấu hình từ user
            var serverPort = GetServerPort();  // Port server (mặc định 1883)
            var serverIP = GetServerIP();      // IP address của máy server

            #region Config MQTT SERVER

            var mqttFactory = new MqttFactory(); // MqttFactory là factory pattern - Tạo ra các đối tượng MQTT

            var mqttServerOptions = mqttFactory.CreateServerOptionsBuilder()
                .WithDefaultEndpoint()                    // Sử dụng endpoint mặc định (TCP)
                .WithDefaultEndpointPort(serverPort)      // Thiết lập port
                .Build();
            using var mqttServer = mqttFactory.CreateMqttServer(mqttServerOptions);  // using = tự động dispose khi kết thúc scope, giải phóng tài nguyên

            #endregion Config MQTT SERVER

            #region Event Handlers

            // Client kết nối thành công
            mqttServer.ClientConnectedAsync += OnClientConnected;

            // Client ngắt kết nối (disconnect)
            mqttServer.ClientDisconnectedAsync += OnClientDisconnected;

            // Client subscribe (đăng ký nhận tin) một topic
            mqttServer.ClientSubscribedTopicAsync += OnClientSubscribed;

            // Client unsubscribe (hủy đăng ký) một topic
            mqttServer.ClientUnsubscribedTopicAsync += OnClientUnsubscribed;

            // Validate (kiểm tra) kết nối
            mqttServer.ValidatingConnectionAsync += OnValidateConnection;

            // Xử lý và chuyển tiếp tin nhắn
            mqttServer.InterceptingPublishAsync += OnMessagePublish;

            #endregion Event Handlers

            #region Server Handler

            try
            {
                // Khởi động server
                await mqttServer.StartAsync();

                // Hiển thị thông tin server đã khởi động thành công
                Console.WriteLine("MQTT BROKER KHỞI ĐỘNG THÀNH CÔNG!");
                Console.WriteLine($"🌐 Server Address: {serverIP}:{serverPort}");
                Console.WriteLine($"📡 Listening on port: {serverPort}");
                Console.WriteLine($"🔗 Clients can connect to: {Environment.MachineName}:{serverPort}");
                Console.WriteLine("📊 Real-time stats will be displayed below");
                Console.WriteLine(new string('=', 70));

                // Khởi động task hiển thị thống kê real-time (chạy song song)
                _ = Task.Run(DisplayRealtimeStats);

                // Stop server
                Console.WriteLine("\n⌨️  Press ENTER to stop the server...");
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                // Xử lý lỗi khi khởi động server
                Console.WriteLine($"❌ Error starting MQTT server: {ex.Message}");
                Console.WriteLine("💡 Try running as Administrator or check if port is available");
            }
            finally
            {
                Console.WriteLine("\n🔄 Stopping MQTT Broker...");
                await mqttServer.StopAsync();
                ShowFinalStats();
                Console.WriteLine("✅ MQTT Broker stopped successfully!");
            }

            #endregion Server Handler
        }

        #endregion Main Function

        #region Feature Function

        /// <summary>
        /// Hàm này được gọi mỗi khi có client publish (gửi) tin nhắn
        /// Nhiệm vụ: Nhận tin nhắn từ Publisher và cho phép broker chuyển tiếp đến Subscribers
        /// </summary>
        /// <param name="e">Event args chứa thông tin tin nhắn</param>
        private static Task OnMessagePublish(InterceptingPublishEventArgs e)
        {
            try
            {
                // ID của client gửi tin nhắn
                var clientId = e.ClientId;

                // Lấy topic tin nhắn được gửi đến
                var topic = e.ApplicationMessage.Topic;

                // Chuyển đổi payload từ byte array sang string
                // PayloadSegment.Count > 0 = kiểm tra có dữ liệu không
                var payload = e.ApplicationMessage.PayloadSegment.Count > 0
                    ? Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment)  // Chuyển bytes thành string UTF-8
                    : string.Empty;  // Nếu không có payload thì để empty

                // Counter
                Interlocked.Increment(ref totalMessagesProcessed);

                // Timestamp log
                var timestamp = DateTime.Now.ToString("HH:mm:ss");

                Console.WriteLine($"📨 [{timestamp}] FROM '{clientId}' TO '{topic}': {payload}");

                // Client Update 
                if (ConnectedClients.TryGetValue(clientId, out var clientInfo)) // Tìm client trong danh sách đã kết nối
                {
                    // Tăng số tin nhắn client đã gửi
                    clientInfo.MessagesSent++;

                    // Cập nhật thời gian hoạt động cuối
                    clientInfo.LastActivity = DateTime.Now;
                }
                e.ProcessPublish = true;
            }
            catch (Exception ex)
            {
                // Xử lý lỗi - log lỗi nhưng vẫn cho tin nhắn đi qua
                Console.WriteLine($"❌ Error processing message: {ex.Message}");

                // Vẫn cho phép tin nhắn đi qua nếu có lỗi
                // Tránh việc một lỗi nhỏ làm gián đoạn toàn bộ hệ thống
                e.ProcessPublish = true;
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Xử lý khi có client kết nối thành công đến broker
        /// </summary>
        /// <param name="e">Thông tin về client vừa kết nối</param>
        private static Task OnClientConnected(ClientConnectedEventArgs e)
        {
            // Lấy thông tin client
            var clientId = e.ClientId;
            var endpoint = e.Endpoint ?? "Unknown";  // IP:Port của client, nếu null thì "Unknown"
            var clientInfo = new ClientInfo
            {
                ClientId = clientId,
                ConnectedAt = DateTime.Now,
                LastActivity = DateTime.Now,
                Endpoint = endpoint.ToString()
            };

            // Thêm client vào danh sách clients đang kết nối
            // TryAdd = thread-safe add, không add nếu key đã tồn tại
            ConnectedClients.TryAdd(clientId, clientInfo);
            Interlocked.Increment(ref totalConnectionsServed);

            Console.WriteLine($"✅ Client CONNECTED: '{clientId}' from {endpoint}");

            return Task.CompletedTask;
        }

        /// <summary>
        /// Xử lý khi client ngắt kết nối (disconnect)
        /// </summary>
        /// <param name="e">Thông tin về client vừa disconnect</param>
        private static Task OnClientDisconnected(ClientDisconnectedEventArgs e)
        {
            var clientId = e.ClientId;

            // Xóa client khỏi danh sách clients đang kết nối
            // TryRemove = thread-safe remove, trả về client info nếu xóa thành công
            ConnectedClients.TryRemove(clientId, out var clientInfo);

            // Xóa client khỏi TẤT CẢ topic subscriptions
            foreach (var topicSubs in TopicSubscribers.Values)
            {
                var remainingClients = topicSubs.Where(c => c != clientId).ToArray();
                topicSubs.Clear();
                foreach (var client in remainingClients)
                {
                    topicSubs.Add(client);
                }
            }

            // Tính thời gian client đã kết nối
            var connectionDuration = clientInfo != null
                ? (DateTime.Now - clientInfo.ConnectedAt).TotalMinutes.ToString("F1") : "Unknown";

            Console.WriteLine($"❌ Client DISCONNECTED: '{clientId}' (connected for {connectionDuration}min)");

            return Task.CompletedTask;
        }

        /// <summary>
        /// Xử lý khi client subscribe (đăng ký nhận tin) một topic
        /// </summary>
        /// <param name="e">Thông tin về subscription</param>
        private static Task OnClientSubscribed(ClientSubscribedTopicEventArgs e)
        {
            var clientId = e.ClientId;
            var topic = e.TopicFilter.Topic;  // Tên topic client muốn subscribe

            // Thêm client vào danh sách subscribers của topic này
            TopicSubscribers.AddOrUpdate(
                topic,
                new ConcurrentBag<string> { clientId },
                (key, existing) => {
                    existing.Add(clientId);
                    return existing;
                }
            );

            Console.WriteLine($"📺 Client '{clientId}' SUBSCRIBED to topic: '{topic}'");

            return Task.CompletedTask;
        }

        /// <summary>
        /// Xử lý khi client unsubscribe (hủy đăng ký) một topic
        /// </summary>
        /// <param name="e">Thông tin về unsubscription</param>
        private static Task OnClientUnsubscribed(ClientUnsubscribedTopicEventArgs e)
        {
            var clientId = e.ClientId;
            var topic = e.TopicFilter;  // Topic client muốn unsubscribe

            if (TopicSubscribers.TryGetValue(topic, out var subscribers))
            {
                var remainingClients = subscribers.Where(c => c != clientId).ToArray();
                subscribers.Clear();
                foreach (var client in remainingClients)
                {
                    subscribers.Add(client);
                }
            }

            Console.WriteLine($"📺 Client '{clientId}' UNSUBSCRIBED from topic: '{topic}'");

            return Task.CompletedTask;
        }

        /// <summary>
        /// Validate kết nối client
        /// </summary>
        /// <param name="e">Thông tin về connection cần validate</param>
        private static Task OnValidateConnection(ValidatingConnectionEventArgs e)
        {
            var clientId = e.ClientId;
            var endpoint = e.Endpoint;

            // TODO: Có thể thêm logic authentication ở đây:
            // - Kiểm tra username/password
            // - Kiểm tra IP whitelist
            // - Kiểm tra certificate
            // - Rate limiting
            // - etc.

            // Hiện tại cho phép tất cả kết nối (no authentication)
            Console.WriteLine($"🔍 Validating connection from '{clientId}' at {endpoint}...");

            e.ReasonCode = MQTTnet.Protocol.MqttConnectReasonCode.Success;

            return Task.CompletedTask;
        }

        /// <summary>
        /// Hiển thị banner khởi động server
        /// </summary>
        private static void ShowBanner()
        {
            Console.Clear();  // Xóa màn hình
            Console.ForegroundColor = ConsoleColor.Cyan;  // Đổi màu chữ
            Console.WriteLine(@"
╔══════════════════════════════════════════════════════════════╗
║  🎯Tính năng:                                                ║
║     ✅ Full MQTT Protocol Support                            ║
║     ✅ Real-time Message Forwarding                          ║
║     ✅ Topic Management                                      ║
║     ✅ Client Connection Tracking                            ║
║     ✅ Live Statistics & Monitoring                          ║
║     ✅ Thread-safe & High Performance                        ║
╚══════════════════════════════════════════════════════════════╝");
            Console.ResetColor();
            Console.WriteLine();
        }

        /// <summary>
        /// Lấy port server từ user input hoặc dùng mặc định 1883
        /// </summary>
        /// <returns>Port number để server lắng nghe</returns>
        private static int GetServerPort()
        {
            Console.Write("📡 Enter MQTT server port (default: 1883): ");
            var portInput = Console.ReadLine();
            // Kiểm tra port hợp lệ (1-65535)
            if (int.TryParse(portInput, out var port) && port > 0 && port <= 65535)
                return port;

            return 1883;  // Default MQTT port
        }

        /// <summary>
        /// Lấy IP address của server
        /// </summary>
        /// <returns>IP address dạng string</returns>
        private static string GetServerIP()
        {
            try
            {
                var host = Dns.GetHostEntry(Dns.GetHostName());
                var ipAddress = host.AddressList
                    .FirstOrDefault(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);

                return ipAddress?.ToString() ?? "localhost";
            }
            catch
            {
                return "localhost";
            }
        }

        /// <summary>
        /// Task chạy background để hiển thị thống kê theo thời gian thực
        /// </summary>
        private static async Task DisplayRealtimeStats()
        {
            while (true)
            {
                // Chờ 10 giây trước mỗi lần update
                await Task.Delay(10000);

                // Tính toán các metrics
                var activeClients = ConnectedClients.Count;  // Số client đang kết nối
                var activeTopics = TopicSubscribers.Count(kv => kv.Value.Count > 0);  // Số topic có subscriber

                // Hiển thị stats tổng quan
                Console.WriteLine($"\n📊 [STATS] Clients: {activeClients} | Topics: {activeTopics} | Messages: {totalMessagesProcessed} | Total Connections: {totalConnectionsServed}");

                // Hiển thị top active clients (gửi tin nhắn nhiều nhất)
                if (activeClients > 0)
                {
                    var topClients = ConnectedClients.Values
                        .OrderByDescending(c => c.MessagesSent)  // Sắp xếp theo số tin nhắn gửi (giảm dần)
                        .Take(3)                                 // Lấy top 3
                        .Select(c => $"{c.ClientId}({c.MessagesSent}msg)")  // Format: ClientId(số tin nhắn)
                        .ToArray();

                    if (topClients.Length > 0)
                        Console.WriteLine($"🏆 Top Active: {string.Join(", ", topClients)}");
                }
            }
        }

        /// <summary>
        /// Hiển thị thống kê tổng kết khi tắt server
        /// </summary>
        private static void ShowFinalStats()
        {
            Console.WriteLine($"\n📈 FINAL STATISTICS:");
            Console.WriteLine($"   Total Connections Served: {totalConnectionsServed}");
            Console.WriteLine($"   Total Messages Processed: {totalMessagesProcessed}");
            Console.WriteLine($"   Peak Concurrent Clients: {ConnectedClients.Count}");
            Console.WriteLine($"   Topics Created: {TopicSubscribers.Count}");
        }

        #endregion Feature Function
    }
}
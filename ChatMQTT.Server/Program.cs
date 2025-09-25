// ==============================================================
// 🏢 CUSTOM MQTT BROKER SERVER - TỰ DỰNG RIÊNG
// ==============================================================
// Đây là một MQTT Broker tự tạo sử dụng thư viện MQTTnet
// MQTT Broker hoạt động như một trung gian (middleman) để:
// - Nhận tin nhắn từ các Publisher (người gửi)
// - Chuyển tiếp tin nhắn đến các Subscriber (người nhận)
// - Quản lý kết nối của các client
// - Theo dõi các topic (chủ đề) và ai đang subscribe

using MQTTnet;               // Thư viện chính của MQTT - cung cấp các class cơ bản
using MQTTnet.Server;        // Các class đặc biệt cho MQTT Server (Broker)
using System.Text;           // Để chuyển đổi giữa string và byte array
using System.Collections.Concurrent; // Thread-safe collections - an toàn khi nhiều thread truy cập
using System.Net;            // Network utilities - để lấy IP address

namespace ChatMQTT.Broker
{
    class Program
    {
        // ==============================================================
        // 📊 CÁC BIẾN TOÀN CỤC - LƯU TRẠNG THÁI SERVER
        // ==============================================================

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

        // Biến đếm tổng số tin nhắn đã xử lý - dùng cho thống kê
        private static int totalMessagesProcessed = 0;

        // Biến đếm tổng số kết nối đã phục vụ (kể cả đã disconnect)
        private static int totalConnectionsServed = 0;

        // ==============================================================
        // 🚀 HÀM MAIN - ĐIỂM KHỞI ĐẦU CỦA CHƯƠNG TRÌNH
        // ==============================================================
        static async Task Main(string[] args)
        {
            // Hiển thị banner chào mừng
            ShowBanner();

            // Lấy thông tin cấu hình từ user
            var serverPort = GetServerPort();  // Port mà server sẽ lắng nghe (mặc định 1883)
            var serverIP = GetServerIP();      // IP address của máy server

            // ==============================================================
            // 🏗️ TẠO VÀ CẤU HÌNH MQTT SERVER
            // ==============================================================

            // MqttFactory là factory pattern - tạo ra các đối tượng MQTT
            var mqttFactory = new MqttFactory();

            // Tạo cấu hình cho MQTT Server
            var mqttServerOptions = mqttFactory.CreateServerOptionsBuilder()
                .WithDefaultEndpoint()                    // Sử dụng endpoint mặc định (TCP)
                .WithDefaultEndpointPort(serverPort)      // Thiết lập port lắng nghe
                .Build();                                 // Build ra đối tượng MqttServerOptions

            // Tạo MQTT Server instance với cấu hình vừa tạo
            // using = tự động dispose khi kết thúc scope, giải phóng tài nguyên
            using var mqttServer = mqttFactory.CreateMqttServer(mqttServerOptions);

            // ==============================================================
            // 📡 ĐĂNG KÝ CÁC EVENT HANDLERS - XỬ LÝ CÁC SỰ KIỆN
            // ==============================================================

            // Khi có client kết nối thành công
            mqttServer.ClientConnectedAsync += OnClientConnected;

            // Khi client ngắt kết nối (disconnect)
            mqttServer.ClientDisconnectedAsync += OnClientDisconnected;

            // Khi client subscribe (đăng ký nhận tin) một topic
            mqttServer.ClientSubscribedTopicAsync += OnClientSubscribed;

            // Khi client unsubscribe (hủy đăng ký) một topic
            mqttServer.ClientUnsubscribedTopicAsync += OnClientUnsubscribed;

            // Khi cần validate (kiểm tra) kết nối - có thể thêm authentication
            mqttServer.ValidatingConnectionAsync += OnValidateConnection;

            // ⭐ QUAN TRỌNG NHẤT: Khi có tin nhắn được publish (gửi đến broker)
            // Đây là trái tim của broker - xử lý và chuyển tiếp tin nhắn
            mqttServer.InterceptingPublishAsync += OnMessagePublish;

            // ==============================================================
            // 🎯 KHỞI ĐỘNG SERVER VÀ XỬ LÝ
            // ==============================================================
            try
            {
                // Khởi động server - bắt đầu lắng nghe kết nối
                await mqttServer.StartAsync();

                // Hiển thị thông tin server đã khởi động thành công
                Console.WriteLine("🎉 MQTT BROKER KHỞI ĐỘNG THÀNH CÔNG!");
                Console.WriteLine($"🌐 Server Address: {serverIP}:{serverPort}");
                Console.WriteLine($"📡 Listening on port: {serverPort}");
                Console.WriteLine($"🔗 Clients can connect to: {Environment.MachineName}:{serverPort}");
                Console.WriteLine("📊 Real-time stats will be displayed below");
                Console.WriteLine(new string('=', 70));

                // Khởi động task hiển thị thống kê real-time (chạy song song)
                // _ = nghĩa là không quan tâm đến return value
                _ = Task.Run(DisplayRealtimeStats);

                // Chờ user nhấn ENTER để tắt server
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
                // Block finally luôn chạy dù có lỗi hay không
                // Dọn dẹp và tắt server một cách graceful
                Console.WriteLine("\n🔄 Stopping MQTT Broker...");
                await mqttServer.StopAsync();
                ShowFinalStats();
                Console.WriteLine("✅ MQTT Broker stopped successfully!");
            }
        }

        // ==============================================================
        // ⭐ HÀM QUAN TRỌNG NHẤT - XỬ LÝ TIN NHẮN ĐƯỢC PUBLISH
        // ==============================================================
        /// <summary>
        /// Đây là trái tim của MQTT Broker!
        /// Hàm này được gọi mỗi khi có client publish (gửi) tin nhắn
        /// Nhiệm vụ: Nhận tin nhắn từ Publisher và cho phép broker chuyển tiếp đến Subscribers
        /// </summary>
        /// <param name="e">Event args chứa thông tin tin nhắn</param>
        private static Task OnMessagePublish(InterceptingPublishEventArgs e)
        {
            try
            {
                // ==============================================================
                // 📨 TRÍCH XUẤT THÔNG TIN TIN NHẮN
                // ==============================================================

                // Lấy ID của client gửi tin nhắn
                var clientId = e.ClientId;

                // Lấy topic (chủ đề) mà tin nhắn được gửi đến
                var topic = e.ApplicationMessage.Topic;

                // Chuyển đổi payload từ byte array sang string
                // PayloadSegment.Count > 0 = kiểm tra có dữ liệu không
                var payload = e.ApplicationMessage.PayloadSegment.Count > 0
                    ? Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment)  // Chuyển bytes thành string UTF-8
                    : string.Empty;  // Nếu không có payload thì để empty

                // ==============================================================
                // 📈 CẬP NHẬT THỐNG KÊ
                // ==============================================================

                // Tăng counter tổng số tin nhắn đã xử lý
                // Interlocked.Increment = thread-safe increment, tránh race condition
                Interlocked.Increment(ref totalMessagesProcessed);

                // Tạo timestamp để log
                var timestamp = DateTime.Now.ToString("HH:mm:ss");

                // Log tin nhắn ra console để admin theo dõi
                Console.WriteLine($"📨 [{timestamp}] FROM '{clientId}' TO '{topic}': {payload}");

                // ==============================================================
                // 👤 CẬP NHẬT THÔNG TIN CLIENT
                // ==============================================================

                // Tìm client trong danh sách đã kết nối
                if (ConnectedClients.TryGetValue(clientId, out var clientInfo))
                {
                    // Tăng số tin nhắn client này đã gửi
                    clientInfo.MessagesSent++;

                    // Cập nhật thời gian hoạt động cuối
                    clientInfo.LastActivity = DateTime.Now;
                }

                // ==============================================================
                // ✅ CHO PHÉP TIN NHẮN ĐƯỢC CHUYỂN TIẾP
                // ==============================================================

                // Đây là phần quan trọng!
                // ProcessPublish = true nghĩa là cho phép broker chuyển tiếp tin nhắn này
                // đến tất cả clients đã subscribe topic này
                // Nếu set = false thì tin nhắn sẽ bị block, không ai nhận được
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

            // Return Task.CompletedTask vì đây là async method nhưng không cần await gì
            return Task.CompletedTask;
        }

        // ==============================================================
        // 🔌 XỬ LÝ KẾT NỐI CLIENT
        // ==============================================================
        /// <summary>
        /// Xử lý khi có client kết nối thành công đến broker
        /// </summary>
        /// <param name="e">Thông tin về client vừa kết nối</param>
        private static Task OnClientConnected(ClientConnectedEventArgs e)
        {
            // Lấy thông tin client
            var clientId = e.ClientId;
            var endpoint = e.Endpoint ?? "Unknown";  // IP:Port của client, nếu null thì "Unknown"

            // Tạo object lưu thông tin chi tiết về client này
            var clientInfo = new ClientInfo
            {
                ClientId = clientId,                    // ID của client
                ConnectedAt = DateTime.Now,             // Thời điểm kết nối
                LastActivity = DateTime.Now,            // Lần hoạt động cuối (ban đầu = lúc kết nối)
                Endpoint = endpoint.ToString()          // Địa chỉ IP:Port
            };

            // Thêm client vào danh sách clients đang kết nối
            // TryAdd = thread-safe add, không add nếu key đã tồn tại
            ConnectedClients.TryAdd(clientId, clientInfo);

            // Tăng counter tổng số kết nối đã phục vụ
            Interlocked.Increment(ref totalConnectionsServed);

            // Log thông tin client kết nối
            Console.WriteLine($"✅ Client CONNECTED: '{clientId}' from {endpoint}");

            return Task.CompletedTask;
        }

        // ==============================================================
        // 🔌 XỬ LÝ NGẮT KẾT NỐI CLIENT
        // ==============================================================
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

            // ==============================================================
            // 🧹 DỌN DẸP SUBSCRIPTIONS
            // ==============================================================

            // Xóa client khỏi TẤT CẢ topic subscriptions
            // Vì client đã disconnect thì không thể nhận tin nhắn nữa
            foreach (var topicSubs in TopicSubscribers.Values)
            {
                // Tạo array chứa tất cả clients trừ client vừa disconnect
                var remainingClients = topicSubs.Where(c => c != clientId).ToArray();

                // Xóa hết rồi thêm lại những client còn lại
                topicSubs.Clear();
                foreach (var client in remainingClients)
                {
                    topicSubs.Add(client);
                }
            }

            // Tính thời gian client đã kết nối
            var connectionDuration = clientInfo != null
                ? (DateTime.Now - clientInfo.ConnectedAt).TotalMinutes.ToString("F1")  // F1 = 1 chữ số thập phân
                : "Unknown";

            // Log thông tin client disconnect
            Console.WriteLine($"❌ Client DISCONNECTED: '{clientId}' (connected for {connectionDuration}min)");

            return Task.CompletedTask;
        }

        // ==============================================================
        // 📺 XỬ LÝ SUBSCRIBE TOPIC
        // ==============================================================
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
                topic,                                          // Key: topic name
                new ConcurrentBag<string> { clientId },         // Value nếu topic chưa tồn tại: tạo mới với client này
                (key, existing) => {                            // Value nếu topic đã tồn tại: thêm client vào list hiện tại
                    existing.Add(clientId);
                    return existing;
                }
            );

            // Log thông tin subscription
            Console.WriteLine($"📺 Client '{clientId}' SUBSCRIBED to topic: '{topic}'");

            return Task.CompletedTask;
        }

        // ==============================================================
        // 📺 XỬ LÝ UNSUBSCRIBE TOPIC
        // ==============================================================
        /// <summary>
        /// Xử lý khi client unsubscribe (hủy đăng ký) một topic
        /// </summary>
        /// <param name="e">Thông tin về unsubscription</param>
        private static Task OnClientUnsubscribed(ClientUnsubscribedTopicEventArgs e)
        {
            var clientId = e.ClientId;
            var topic = e.TopicFilter;  // Topic client muốn unsubscribe

            // Tìm danh sách subscribers của topic này
            if (TopicSubscribers.TryGetValue(topic, out var subscribers))
            {
                // Tạo list mới không chứa client này
                var remainingClients = subscribers.Where(c => c != clientId).ToArray();

                // Clear và add lại những client còn lại
                subscribers.Clear();
                foreach (var client in remainingClients)
                {
                    subscribers.Add(client);
                }
            }

            // Log thông tin unsubscription
            Console.WriteLine($"📺 Client '{clientId}' UNSUBSCRIBED from topic: '{topic}'");

            return Task.CompletedTask;
        }

        // ==============================================================
        // 🔒 VALIDATE KẾT NỐI - AUTHENTICATION
        // ==============================================================
        /// <summary>
        /// Validate kết nối client - có thể thêm authentication logic ở đây
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

            // ReasonCode = Success nghĩa là chấp nhận kết nối
            // Có thể set thành BadUserNameOrPassword, NotAuthorized, etc. để reject
            e.ReasonCode = MQTTnet.Protocol.MqttConnectReasonCode.Success;

            return Task.CompletedTask;
        }

        // ==============================================================
        // 🎨 HIỂN THỊ BANNER KHỞI ĐỘNG
        // ==============================================================
        /// <summary>
        /// Hiển thị banner chào mừng khi khởi động server
        /// </summary>
        private static void ShowBanner()
        {
            Console.Clear();  // Xóa màn hình
            Console.ForegroundColor = ConsoleColor.Cyan;  // Đổi màu chữ

            // Raw string literal với @ - không cần escape các ký tự đặc biệt
            Console.WriteLine(@"
╔══════════════════════════════════════════════════════════════╗
║              🏢 CUSTOM MQTT BROKER SERVER                    ║
║                    Tự dựng riêng bởi HaiDang                 ║
║                                                              ║
║  🎯 Tính năng:                                               ║
║     ✅ Full MQTT Protocol Support                            ║
║     ✅ Real-time Message Forwarding                          ║
║     ✅ Topic Management                                       ║
║     ✅ Client Connection Tracking                            ║
║     ✅ Live Statistics & Monitoring                          ║
║     ✅ Thread-safe & High Performance                        ║
╚══════════════════════════════════════════════════════════════╝");

            Console.ResetColor();  // Reset màu chữ về mặc định
            Console.WriteLine();
        }

        // ==============================================================
        // ⚙️ LẤY THÔNG TIN CẤU HÌNH
        // ==============================================================
        /// <summary>
        /// Lấy port server từ user input hoặc dùng mặc định 1883
        /// </summary>
        /// <returns>Port number để server lắng nghe</returns>
        private static int GetServerPort()
        {
            Console.Write("📡 Enter MQTT server port (default: 1883): ");
            var portInput = Console.ReadLine();

            // Thử parse input thành integer
            // Kiểm tra port hợp lệ (1-65535)
            if (int.TryParse(portInput, out var port) && port > 0 && port <= 65535)
                return port;

            return 1883;  // Default MQTT port
        }

        /// <summary>
        /// Lấy IP address của server (máy đang chạy)
        /// </summary>
        /// <returns>IP address dạng string</returns>
        private static string GetServerIP()
        {
            try
            {
                // Lấy thông tin host của máy hiện tại
                var host = Dns.GetHostEntry(Dns.GetHostName());

                // Tìm IPv4 address đầu tiên (loại bỏ IPv6 và loopback)
                var ipAddress = host.AddressList
                    .FirstOrDefault(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);

                return ipAddress?.ToString() ?? "localhost";
            }
            catch
            {
                // Nếu có lỗi thì dùng localhost
                return "localhost";
            }
        }

        // ==============================================================
        // 📊 HIỂN THỊ THỐNG KÊ REAL-TIME
        // ==============================================================
        /// <summary>
        /// Task chạy background để hiển thị thống kê theo thời gian thực
        /// </summary>
        private static async Task DisplayRealtimeStats()
        {
            // Vòng lặp vô tận để update stats
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

        // ==============================================================
        // 📈 HIỂN THỊ THỐNG KÊ CUỐI CÙNG
        // ==============================================================
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
    }

    // ==============================================================
    // 📋 CLASS LƯU THÔNG TIN CLIENT
    // ==============================================================
    /// <summary>
    /// Class chứa thông tin chi tiết về một client kết nối
    /// </summary>
    public class ClientInfo
    {
        /// <summary>ID duy nhất của client</summary>
        public string ClientId { get; set; } = string.Empty;

        /// <summary>Thời điểm client kết nối đến server</summary>
        public DateTime ConnectedAt { get; set; }

        /// <summary>Lần hoạt động cuối cùng (gửi tin nhắn, subscribe, etc.)</summary>
        public DateTime LastActivity { get; set; }

        /// <summary>Tổng số tin nhắn client này đã gửi</summary>
        public int MessagesSent { get; set; } = 0;

        /// <summary>Địa chỉ endpoint của client (IP:Port)</summary>
        public string Endpoint { get; set; } = string.Empty;
    }
}
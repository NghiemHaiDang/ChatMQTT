// ==============================================================
// 💻 MQTT CLIENT - KẾT NỐI VÀ CHAT QUA MQTT BROKER
// ==============================================================
// Đây là ứng dụng MQTT Client để chat real-time qua MQTT protocol
// Client này có thể:
// - Kết nối đến MQTT Broker
// - Subscribe (đăng ký nhận tin) từ các topic
// - Publish (gửi tin nhắn) đến các topic
// - Hiển thị lịch sử tin nhắn
// - Quản lý kết nối một cách user-friendly

using MQTTnet;               // Thư viện chính của MQTT - cung cấp các class cơ bản
using MQTTnet.Client;        // Các class đặc biệt cho MQTT Client
using System.Text;           // Để chuyển đổi giữa string và byte array
using System.Text.Json;      // Để serialize/deserialize JSON cho tin nhắn

namespace ChatMQTT.Client
{
    class Program
    {
        // ==============================================================
        // 📊 CÁC BIẾN TOÀN CỤC - LƯU TRẠNG THÁI CLIENT
        // ==============================================================

        /// <summary>
        /// Instance của MQTT Client - đối tượng chính để giao tiếp với broker
        /// IMqttClient là interface, cho phép mock testing và loose coupling
        /// </summary>
        private static IMqttClient? mqttClient;

        /// <summary>
        /// ID duy nhất của client này - được tạo tự động hoặc user nhập
        /// </summary>
        private static string? clientId;

        /// <summary>
        /// Topic hiện tại mà client đang subscribe (nhận tin nhắn)
        /// Null = chưa subscribe topic nào
        /// </summary>
        private static string? currentTopic;

        /// <summary>
        /// Danh sách lưu lịch sử tất cả tin nhắn đã gửi và nhận
        /// List<T> không thread-safe nhưng ở đây chỉ có 1 thread chính nên OK
        /// </summary>
        private static readonly List<ChatMessage> messageHistory = new();

        // ==============================================================
        // 🚀 HÀM MAIN - ĐIỂM KHỞI ĐẦU CỦA CHƯƠNG TRÌNH
        // ==============================================================
        static async Task Main(string[] args)
        {
            // Hiển thị banner chào mừng
            ShowBanner();

            // ==============================================================
            // 📋 THU THẬP THÔNG TIN TỪ USER
            // ==============================================================

            // Lấy thông tin MQTT broker (host, port) từ user
            var serverInfo = GetServerInfo();

            // Lấy thông tin user (username, tạo clientId)
            var userInfo = GetUserInfo();

            // ==============================================================
            // 🔌 TẠO VÀ KẾT NỐI MQTT CLIENT
            // ==============================================================

            // Tạo kết nối đến broker với thông tin vừa thu thập
            await CreateAndConnectClient(serverInfo, userInfo);

            // ==============================================================
            // 🎯 HIỂN THỊ MENU CHÍNH VÀ XỬ LÝ
            // ==============================================================

            // Vào vòng lặp menu chính để user tương tác
            await ShowMainMenu();
        }

        // ==============================================================
        // 🔌 TẠO VÀ KẾT NỐI CLIENT
        // ==============================================================
        /// <summary>
        /// Tạo MQTT client và kết nối đến broker
        /// </summary>
        /// <param name="serverInfo">Thông tin broker (host, port)</param>
        /// <param name="userInfo">Thông tin user (username, clientId)</param>
        private static async Task CreateAndConnectClient(ServerInfo serverInfo, UserInfo userInfo)
        {
            try
            {
                // ==============================================================
                // 🏗️ TẠO MQTT CLIENT
                // ==============================================================

                // MqttFactory dùng factory pattern để tạo các đối tượng MQTT
                var mqttFactory = new MqttFactory();

                // Tạo MQTT Client instance
                mqttClient = mqttFactory.CreateMqttClient();

                // ==============================================================
                // 📡 ĐĂNG KÝ CÁC EVENT HANDLERS
                // ==============================================================

                // Event được trigger khi kết nối thành công
                mqttClient.ConnectedAsync += OnConnected;

                // Event được trigger khi mất kết nối
                mqttClient.DisconnectedAsync += OnDisconnected;

                // ⭐ Event QUAN TRỌNG NHẤT: khi nhận được tin nhắn
                // Đây là trái tim của client - xử lý tin nhắn nhận được
                mqttClient.ApplicationMessageReceivedAsync += OnMessageReceived;

                // ==============================================================
                // ⚙️ CẤU HÌNH KẾT NỐI
                // ==============================================================

                // Tạo cấu hình kết nối với các thông số cần thiết
                var mqttClientOptions = mqttFactory.CreateClientOptionsBuilder()
                    .WithTcpServer(serverInfo.Host, serverInfo.Port)  // Địa chỉ broker
                    .WithClientId(userInfo.ClientId)                  // ID duy nhất của client
                    .WithCleanSession()                               // Clean session = không lưu state cũ
                    .Build();

                // ==============================================================
                // 🔗 THỰC HIỆN KẾT NỐI
                // ==============================================================

                Console.WriteLine($"\n🔄 Connecting to MQTT broker at {serverInfo.Host}:{serverInfo.Port}...");

                // Kết nối đến broker (async operation)
                await mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None);

                // Lưu clientId để sử dụng sau này
                clientId = userInfo.ClientId;

                Console.WriteLine($"✅ Connected successfully as '{clientId}'!");
            }
            catch (Exception ex)
            {
                // Nếu kết nối thất bại thì thoát chương trình
                Console.WriteLine($"❌ Connection failed: {ex.Message}");
                Environment.Exit(1);  // Exit code 1 = error
            }
        }

        // ==============================================================
        // 🎯 MENU CHÍNH - GIAO DIỆN NGƯỜI DÙNG
        // ==============================================================
        /// <summary>
        /// Hiển thị menu chính và xử lý lựa chọn của user
        /// Chạy trong vòng lặp cho đến khi user chọn thoát
        /// </summary>
        private static async Task ShowMainMenu()
        {
            // Vòng lặp menu chính - chỉ dừng khi disconnect hoặc thoát
            while (mqttClient?.IsConnected == true)
            {
                // ==============================================================
                // 🖥️ HIỂN THỊ MENU OPTIONS
                // ==============================================================

                Console.WriteLine("\n" + new string('=', 60));
                Console.WriteLine("📋 MAIN MENU:");
                Console.WriteLine("1. 📺 Subscribe to topic");      // Đăng ký nhận tin từ topic
                Console.WriteLine("2. 📨 Send message");           // Gửi tin nhắn đến topic
                Console.WriteLine("3. 📜 View message history");   // Xem lịch sử tin nhắn
                Console.WriteLine("4. 🔄 Change topic");           // Đổi topic khác
                Console.WriteLine("5. 📊 Show connection info");   // Hiển thị thông tin kết nối
                Console.WriteLine("6. ❌ Disconnect and exit");     // Thoát chương trình
                Console.WriteLine(new string('=', 60));

                // ==============================================================
                // ⌨️ NHẬN VÀ XỬ LÝ LỰA CHỌN
                // ==============================================================

                Console.Write("Choose option (1-6): ");
                var choice = Console.ReadLine();

                // Switch statement xử lý từng option
                switch (choice)
                {
                    case "1":
                        await SubscribeToTopic();    // Subscribe topic mới
                        break;
                    case "2":
                        await SendMessage();         // Gửi tin nhắn
                        break;
                    case "3":
                        ShowMessageHistory();        // Hiển thị lịch sử
                        break;
                    case "4":
                        await ChangeTopic();         // Đổi topic
                        break;
                    case "5":
                        ShowConnectionInfo();        // Hiển thị info
                        break;
                    case "6":
                        await DisconnectAndExit();   // Thoát
                        return;                      // Thoát khỏi vòng lặp
                    default:
                        Console.WriteLine("❌ Invalid option! Please choose 1-6.");
                        break;
                }
            }
        }

        // ==============================================================
        // 📺 SUBSCRIBE TO TOPIC - ĐĂNG KÝ NHẬN TIN
        // ==============================================================
        /// <summary>
        /// Cho phép user subscribe (đăng ký nhận tin) từ một topic
        /// Topic trong MQTT như "kênh" hoặc "room" trong chat
        /// </summary>
        private static async Task SubscribeToTopic()
        {
            // Nhận topic name từ user
            Console.Write("\n📺 Enter topic to subscribe (e.g., 'chat/general'): ");
            var topic = Console.ReadLine()?.Trim();

            // Validation: topic không được empty
            if (string.IsNullOrEmpty(topic))
            {
                Console.WriteLine("❌ Topic cannot be empty!");
                return;
            }

            try
            {
                // ==============================================================
                // 🏗️ TẠO TOPIC FILTER
                // ==============================================================

                // MqttTopicFilterBuilder dùng builder pattern để tạo topic filter
                var topicFilter = new MqttTopicFilterBuilder()
                    .WithTopic(topic)  // Tên topic muốn subscribe
                    .Build();          // Build thành MqttTopicFilter object

                // ==============================================================
                // 📡 GỬI SUBSCRIBE REQUEST
                // ==============================================================

                // Gửi subscribe request đến broker
                await mqttClient!.SubscribeAsync(topicFilter, CancellationToken.None);

                // Lưu topic hiện tại để dùng cho send message
                currentTopic = topic;

                Console.WriteLine($"✅ Successfully subscribed to '{topic}'");
                Console.WriteLine("💡 You will now receive messages from this topic");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Failed to subscribe: {ex.Message}");
            }
        }

        // ==============================================================
        // 📨 SEND MESSAGE - GỬI TIN NHẮN
        // ==============================================================
        /// <summary>
        /// Gửi tin nhắn đến topic hiện tại
        /// Tin nhắn được format thành JSON chứa metadata
        /// </summary>
        private static async Task SendMessage()
        {
            // Kiểm tra đã subscribe topic chưa
            if (string.IsNullOrEmpty(currentTopic))
            {
                Console.WriteLine("❌ Please subscribe to a topic first!");
                return;
            }

            // Nhận nội dung tin nhắn từ user
            Console.Write($"\n💬 Enter message to send to '{currentTopic}': ");
            var messageText = Console.ReadLine();

            // Validation: tin nhắn không được empty
            if (string.IsNullOrEmpty(messageText))
            {
                Console.WriteLine("❌ Message cannot be empty!");
                return;
            }

            try
            {
                // ==============================================================
                // 📦 TẠO CHAT MESSAGE OBJECT
                // ==============================================================

                // Tạo object chứa metadata của tin nhắn
                var chatMessage = new ChatMessage
                {
                    From = clientId!,           // Người gửi
                    Message = messageText,      // Nội dung tin nhắn
                    Timestamp = DateTime.Now,   // Thời gian gửi
                    Topic = currentTopic        // Topic đích
                };

                // ==============================================================
                // 🔄 SERIALIZE TO JSON
                // ==============================================================

                // Chuyển object thành JSON string để gửi qua mạng
                var messageJson = JsonSerializer.Serialize(chatMessage);

                // ==============================================================
                // 🏗️ TẠO MQTT APPLICATION MESSAGE
                // ==============================================================

                // MqttApplicationMessageBuilder dùng builder pattern
                var message = new MqttApplicationMessageBuilder()
                    .WithTopic(currentTopic)    // Topic đích
                    .WithPayload(messageJson)   // Nội dung (JSON string)
                    .Build();                   // Build thành MqttApplicationMessage

                // ==============================================================
                // 📡 PUBLISH MESSAGE
                // ==============================================================

                // Gửi tin nhắn đến broker (broker sẽ chuyển tiếp đến subscribers)
                await mqttClient!.PublishAsync(message, CancellationToken.None);

                Console.WriteLine($"✅ Message sent to '{currentTopic}'");

                // ==============================================================
                // 💾 LƯU VÀO LỊCH SỬ
                // ==============================================================

                // Thêm tin nhắn vừa gửi vào lịch sử để user có thể xem lại
                messageHistory.Add(chatMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Failed to send message: {ex.Message}");
            }
        }

        // ==============================================================
        // 📜 SHOW MESSAGE HISTORY - HIỂN THỊ LỊCH SỬ
        // ==============================================================
        /// <summary>
        /// Hiển thị lịch sử tin nhắn (gửi và nhận)
        /// Chỉ hiển thị 20 tin nhắn gần nhất để tránh spam console
        /// </summary>
        private static void ShowMessageHistory()
        {
            Console.WriteLine($"\n📜 MESSAGE HISTORY ({messageHistory.Count} messages):");
            Console.WriteLine(new string('-', 60));

            // Kiểm tra có tin nhắn nào không
            if (messageHistory.Count == 0)
            {
                Console.WriteLine("No messages yet.");
                return;
            }

            // Lấy 20 tin nhắn cuối cùng (mới nhất)
            var last20Messages = messageHistory.TakeLast(20);

            // ==============================================================
            // 🖨️ HIỂN THỊ TỪNG TIN NHẮN
            // ==============================================================

            foreach (var msg in last20Messages)
            {
                // Format thời gian
                var timeStr = msg.Timestamp.ToString("HH:mm:ss");

                // Hiển thị tên người gửi (nếu là mình thì hiển thị "You")
                var fromStr = msg.From == clientId ? "You" : msg.From;

                // Icon chỉ hướng: → = gửi đi, ← = nhận về
                var direction = msg.From == clientId ? "→" : "←";

                // Format: [time] direction from @topic: message
                Console.WriteLine($"[{timeStr}] {direction} {fromStr} @{msg.Topic}: {msg.Message}");
            }
        }

        // ==============================================================
        // 🔄 CHANGE TOPIC - CHUYỂN ĐỔI TOPIC
        // ==============================================================
        /// <summary>
        /// Cho phép user chuyển đổi sang topic khác
        /// Có thể unsubscribe topic cũ trước khi subscribe topic mới
        /// </summary>
        private static async Task ChangeTopic()
        {
            // Kiểm tra có topic hiện tại không
            if (!string.IsNullOrEmpty(currentTopic))
            {
                Console.WriteLine($"📺 Currently subscribed to: '{currentTopic}'");
                Console.Write("Do you want to unsubscribe from current topic? (y/n): ");
                var unsubscribe = Console.ReadLine()?.ToLower();

                // Nếu user muốn unsubscribe topic cũ
                if (unsubscribe == "y" || unsubscribe == "yes")
                {
                    try
                    {
                        // Gửi unsubscribe request đến broker
                        await mqttClient!.UnsubscribeAsync(currentTopic);
                        Console.WriteLine($"✅ Unsubscribed from '{currentTopic}'");
                        currentTopic = null;  // Reset current topic
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"❌ Failed to unsubscribe: {ex.Message}");
                    }
                }
            }

            // Subscribe topic mới
            await SubscribeToTopic();
        }

        // ==============================================================
        // 📊 SHOW CONNECTION INFO - HIỂN THỊ THÔNG TIN KẾT NỐI
        // ==============================================================
        /// <summary>
        /// Hiển thị thông tin tổng quan về kết nối và trạng thái hiện tại
        /// </summary>
        private static void ShowConnectionInfo()
        {
            Console.WriteLine($"\n📊 CONNECTION INFO:");
            Console.WriteLine($"   Client ID: {clientId}");                               // ID của client
            Console.WriteLine($"   Connected: {mqttClient?.IsConnected}");                // Trạng thái kết nối
            Console.WriteLine($"   Current Topic: {currentTopic ?? "None"}");             // Topic đang subscribe
            Console.WriteLine($"   Messages in History: {messageHistory.Count}");        // Số tin nhắn trong lịch sử
        }

        // ==============================================================
        // ❌ DISCONNECT AND EXIT - THOÁT CHƯƠNG TRÌNH
        // ==============================================================
        /// <summary>
        /// Disconnect khỏi broker và thoát chương trình một cách graceful
        /// </summary>
        private static async Task DisconnectAndExit()
        {
            Console.WriteLine("\n🔄 Disconnecting...");

            try
            {
                // Kiểm tra còn kết nối không thì mới disconnect
                if (mqttClient?.IsConnected == true)
                {
                    await mqttClient.DisconnectAsync();
                }
                Console.WriteLine("✅ Disconnected successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error during disconnect: {ex.Message}");
            }

            Console.WriteLine("👋 Goodbye!");
            Environment.Exit(0);  // Exit code 0 = success
        }

        // ==============================================================
        // 📡 EVENT HANDLERS - XỬ LÝ CÁC SỰ KIỆN
        // ==============================================================

        /// <summary>
        /// Event handler được gọi khi kết nối thành công đến broker
        /// </summary>
        /// <param name="e">Event arguments chứa thông tin kết nối</param>
        private static Task OnConnected(MqttClientConnectedEventArgs e)
        {
            Console.WriteLine($"🟢 Connected to MQTT broker!");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Event handler được gọi khi mất kết nối với broker
        /// </summary>
        /// <param name="e">Event arguments chứa lý do disconnect</param>
        private static Task OnDisconnected(MqttClientDisconnectedEventArgs e)
        {
            Console.WriteLine($"🔴 Disconnected from MQTT broker: {e.Reason}");
            return Task.CompletedTask;
        }

        // ==============================================================
        // ⭐ EVENT HANDLER QUAN TRỌNG NHẤT - NHẬN TIN NHẮN
        // ==============================================================
        /// <summary>
        /// Event handler được gọi mỗi khi nhận được tin nhắn từ topic đã subscribe
        /// Đây là trái tim của client - xử lý tin nhắn real-time
        /// </summary>
        /// <param name="e">Event arguments chứa tin nhắn nhận được</param>
        private static Task OnMessageReceived(MqttApplicationMessageReceivedEventArgs e)
        {
            try
            {
                // ==============================================================
                // 📨 TRÍCH XUẤT THÔNG TIN TIN NHẮN
                // ==============================================================

                // Chuyển payload từ byte array thành string
                var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
                var topic = e.ApplicationMessage.Topic;

                // ==============================================================
                // 🔄 THỬ PARSE JSON MESSAGE
                // ==============================================================

                // Thử parse JSON, nếu không được thì hiển thị raw text
                try
                {
                    // Deserialize JSON thành ChatMessage object
                    var chatMessage = JsonSerializer.Deserialize<ChatMessage>(payload);

                    // Kiểm tra tin nhắn hợp lệ và không phải từ chính mình
                    if (chatMessage != null && chatMessage.From != clientId)
                    {
                        // ==============================================================
                        // 💬 HIỂN THỊ TIN NHẮN CHAT
                        // ==============================================================

                        var timeStr = chatMessage.Timestamp.ToString("HH:mm:ss");
                        Console.WriteLine($"\n💬 [{timeStr}] {chatMessage.From} @{topic}: {chatMessage.Message}");

                        // Thêm vào lịch sử để user có thể xem lại
                        messageHistory.Add(chatMessage);
                    }
                    // Nếu tin nhắn từ chính mình thì không hiển thị (tránh duplicate)
                }
                catch
                {
                    // ==============================================================
                    // 📨 HIỂN THỊ RAW MESSAGE (KHÔNG PHẢI JSON)
                    // ==============================================================

                    // Nếu không parse được JSON thì hiển thị raw text
                    var timeStr = DateTime.Now.ToString("HH:mm:ss");
                    Console.WriteLine($"\n📨 [{timeStr}] @{topic}: {payload}");

                    // Tạo ChatMessage object cho raw message
                    var rawMessage = new ChatMessage
                    {
                        From = "Unknown",       // Không biết người gửi
                        Message = payload,      // Raw content
                        Timestamp = DateTime.Now,
                        Topic = topic
                    };
                    messageHistory.Add(rawMessage);
                }

                // ==============================================================
                // 🖥️ RE-SHOW MENU PROMPT
                // ==============================================================

                // Hiển thị lại prompt để user biết có thể nhập lệnh tiếp
                Console.Write("Choose option (1-6): ");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error processing message: {ex.Message}");
            }

            return Task.CompletedTask;
        }

        // ==============================================================
        // 🎨 HELPER METHODS - CÁC HÀM HỖ TRỢ
        // ==============================================================

        /// <summary>
        /// Hiển thị banner chào mừng khi khởi động client
        /// </summary>
        private static void ShowBanner()
        {
            Console.Clear();  // Xóa màn hình
            Console.ForegroundColor = ConsoleColor.Green;  // Đổi màu chữ

            Console.WriteLine(@"
╔══════════════════════════════════════════════════════════════╗
║                💻 MQTT CLIENT APPLICATION                    ║
║                     Chat qua MQTT Protocol                   ║
║                                                              ║
║  🎯 Tính năng:                                               ║
║     ✅ Connect to MQTT Broker                                ║
║     ✅ Subscribe/Unsubscribe Topics                          ║
║     ✅ Send & Receive Messages                               ║
║     ✅ Message History                                        ║
║     ✅ Real-time Chat                                         ║
║     ✅ JSON Message Format                                    ║
╚══════════════════════════════════════════════════════════════╝");

            Console.ResetColor();  // Reset màu chữ về mặc định
            Console.WriteLine();
        }

        /// <summary>
        /// Lấy thông tin MQTT broker từ user (host và port)
        /// </summary>
        /// <returns>ServerInfo object chứa host và port</returns>
        private static ServerInfo GetServerInfo()
        {
            Console.WriteLine("🌐 MQTT BROKER CONNECTION:");

            // ==============================================================
            // 🏠 LẤY HOST ADDRESS
            // ==============================================================

            Console.Write("Enter MQTT broker host (default: localhost): ");
            var host = Console.ReadLine()?.Trim();
            if (string.IsNullOrEmpty(host)) host = "localhost";  // Default value

            // ==============================================================
            // 🔢 LẤY PORT NUMBER
            // ==============================================================

            Console.Write("Enter MQTT broker port (default: 1883): ");
            var portInput = Console.ReadLine()?.Trim();
            var port = 1883;  // Default MQTT port

            // Thử parse port input
            if (!string.IsNullOrEmpty(portInput) && int.TryParse(portInput, out var parsedPort))
            {
                port = parsedPort;
            }

            return new ServerInfo { Host = host, Port = port };
        }

        /// <summary>
        /// Lấy thông tin user (username) và tạo unique client ID
        /// </summary>
        /// <returns>UserInfo object chứa username và clientId</returns>
        private static UserInfo GetUserInfo()
        {
            Console.WriteLine("\n👤 USER INFORMATION:");

            // ==============================================================
            // 👤 LẤY USERNAME
            // ==============================================================

            Console.Write("Enter your username: ");
            var username = Console.ReadLine()?.Trim();

            // Nếu user không nhập gì thì tạo username mặc định
            if (string.IsNullOrEmpty(username))
            {
                username = $"User_{DateTime.Now:HHmmss}";  // Format: User_142530
                Console.WriteLine($"Using default username: {username}");
            }

            // ==============================================================
            // 🆔 TẠO UNIQUE CLIENT ID
            // ==============================================================

            // Client ID phải unique trong toàn broker
            // Format: ChatClient_Username_TimeStamp để đảm bảo unique
            var clientId = $"ChatClient_{username}_{DateTime.Now:HHmmss}";

            return new UserInfo { Username = username, ClientId = clientId };
        }
    }

    // ==============================================================
    // 📋 DATA CLASSES - CÁC CLASS CHỨA DỮ LIỆU
    // ==============================================================

    /// <summary>
    /// Class chứa thông tin về MQTT broker server
    /// </summary>
    public class ServerInfo
    {
        /// <summary>Host address của broker (IP hoặc domain name)</summary>
        public string Host { get; set; } = string.Empty;

        /// <summary>Port number mà broker đang lắng nghe</summary>
        public int Port { get; set; }
    }

    /// <summary>
    /// Class chứa thông tin về user
    /// </summary>
    public class UserInfo
    {
        /// <summary>Tên người dùng (để hiển thị)</summary>
        public string Username { get; set; } = string.Empty;

        /// <summary>Client ID duy nhất (để broker phân biệt các client)</summary>
        public string ClientId { get; set; } = string.Empty;
    }

    /// <summary>
    /// Class đại diện cho một tin nhắn chat
    /// Được serialize thành JSON khi gửi qua MQTT
    /// </summary>
    public class ChatMessage
    {
        /// <summary>Người gửi tin nhắn (Client ID hoặc username)</summary>
        public string From { get; set; } = string.Empty;

        /// <summary>Nội dung tin nhắn</summary>
        public string Message { get; set; } = string.Empty;

        /// <summary>Thời gian gửi tin nhắn</summary>
        public DateTime Timestamp { get; set; }

        /// <summary>Topic mà tin nhắn được gửi đến</summary>
        public string Topic { get; set; } = string.Empty;
    }
}
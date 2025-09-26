// ==============================================================
//MQTT CLIENT - KẾT NỐI VÀ CHAT QUA MQTT BROKER
// =============================================================
// Đây là ứng dụng MQTT Client để chat real-time qua MQTT protocol
// Client này có thể:
// - Kết nối đến MQTT Broker
// - Subscribe (đăng ký nhận tin) từ các topic
// - Publish (gửi tin nhắn) đến các topic
// - Hiển thị lịch sử tin nhắn 
// - Quản lý kết nối một cách user-friendly

using BuildingBlocks.Models.Clients;
using MQTTnet;               
using MQTTnet.Client;       
using System.Text;           
using System.Text.Json;     

namespace ChatMQTT.Client
{
    class Program
    {
        #region Global variables

        private static IMqttClient? mqttClient;

        private static string? clientId;

        private static string? currentTopic;

        private static readonly List<ChatMessage> messageHistory = new();

        #endregion Global variables

        #region Main Function

        static async Task Main(string[] args)
        {
            // Banner
            ShowBanner();

            // Lấy thông tin MQTT broker (host, port) từ user
            var serverInfo = GetServerInfo();

            // Lấy thông tin user (username, tạo clientId)
            var userInfo = GetUserInfo();

            // Tạo kết nối đến broker với thông tin vừa thu thập
            await CreateAndConnectClient(serverInfo, userInfo);

            await ShowMainMenu();
        }

        #endregion Main Function

        #region Feature Function

        /// <summary>
        /// Tạo MQTT client và kết nối đến broker
        /// </summary>
        /// <param name="serverInfo">Thông tin broker (host, port)</param>
        /// <param name="userInfo">Thông tin user (username, clientId)</param>
        private static async Task CreateAndConnectClient(ServerInfo serverInfo, UserInfo userInfo)
        {
            try
            {
                var mqttFactory = new MqttFactory();

                // Tạo MQTT Client instance
                mqttClient = mqttFactory.CreateMqttClient();

                // Event được trigger khi kết nối thành công
                mqttClient.ConnectedAsync += OnConnected;

                // Event được trigger khi mất kết nối
                mqttClient.DisconnectedAsync += OnDisconnected;

                // Xử lý tin nhắn nhận được
                mqttClient.ApplicationMessageReceivedAsync += OnMessageReceived;

                // Tạo cấu hình kết nối
                var mqttClientOptions = mqttFactory.CreateClientOptionsBuilder()
                    .WithTcpServer(serverInfo.Host, serverInfo.Port)  // Địa chỉ broker
                    .WithClientId(userInfo.ClientId)                  // ID duy nhất của client
                    .WithCleanSession()                               // Clean session = không lưu state cũ
                    .Build();

                Console.WriteLine($"\n🔄 Connecting to MQTT broker at {serverInfo.Host}:{serverInfo.Port}...");

                // Kết nối đến broker (async operation)
                await mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None);
                clientId = userInfo.ClientId;

                Console.WriteLine($"✅ Connected successfully as '{clientId}'!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Connection failed: {ex.Message}");
                Environment.Exit(1);
            }
        }

        /// <summary>
        /// Hiển thị menu chính
        /// Chạy trong vòng lặp cho đến khi user chọn thoát
        /// </summary>
        private static async Task ShowMainMenu()
        {
            while (mqttClient?.IsConnected == true)
            {
                Console.WriteLine("\n" + new string('=', 60));
                Console.WriteLine("📋 MAIN MENU:");
                Console.WriteLine("1. 📺 Subscribe to topic");      // Đăng ký nhận tin từ topic
                Console.WriteLine("2. 📨 Send message");            // Gửi tin nhắn đến topic
                Console.WriteLine("3. 📜 View message history");    // Xem lịch sử tin nhắn
                Console.WriteLine("4. 🔄 Change topic");            // Đổi topic khác
                Console.WriteLine("5. 📊 Show connection info");    // Hiển thị thông tin kết nối
                Console.WriteLine("6. ❌ Disconnect and exit");     // Thoát chương trình
                Console.WriteLine(new string('=', 60));

                Console.Write("Choose option (1-6): ");
                var choice = Console.ReadLine();
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

        /// <summary>
        /// Cho phép user subscribe (đăng ký nhận tin) từ một topic
        /// Topic trong MQTT như "kênh" hoặc "room" trong chat
        /// </summary>
        private static async Task SubscribeToTopic()
        {
            Console.Write("\n📺 Enter topic to subscribe (e.g., 'chat/general'): ");
            var topic = Console.ReadLine()?.Trim();

            if (string.IsNullOrEmpty(topic))
            {
                Console.WriteLine("❌ Topic cannot be empty!");
                return;
            }

            try
            {
                var topicFilter = new MqttTopicFilterBuilder()
                    .WithTopic(topic)  // Tên topic muốn subscribe
                    .Build();

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
                // Tạo object chứa metadata của tin nhắn
                var chatMessage = new ChatMessage
                {
                    From = clientId!,           // Người gửi
                    Message = messageText,      // Nội dung tin nhắn
                    Timestamp = DateTime.Now,   // Thời gian gửi
                    Topic = currentTopic        // Topic đích
                };

                // Chuyển object thành JSON string để gửi qua mạng
                var messageJson = JsonSerializer.Serialize(chatMessage);

                // MqttApplicationMessageBuilder dùng builder pattern
                var message = new MqttApplicationMessageBuilder()
                    .WithTopic(currentTopic)    // Topic đích
                    .WithPayload(messageJson)   // Nội dung (JSON string)
                    .Build();                   // Build thành MqttApplicationMessage

                // Gửi tin nhắn đến broker (broker sẽ chuyển tiếp đến subscribers)
                await mqttClient!.PublishAsync(message, CancellationToken.None);

                Console.WriteLine($"✅ Message sent to '{currentTopic}'");

                // Thêm tin nhắn vừa gửi vào lịch sử để user có thể xem lại
                messageHistory.Add(chatMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Failed to send message: {ex.Message}");
            }
        }

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

        /// <summary>
        /// Disconnect khỏi broker và thoát chương trình
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

        /// <summary>
        /// Event handler được gọi mỗi khi nhận được tin nhắn từ topic đã subscribe
        /// </summary>
        /// <param name="e">Event arguments chứa tin nhắn nhận được</param>
        private static Task OnMessageReceived(MqttApplicationMessageReceivedEventArgs e)
        {
            try
            {
                // Chuyển payload từ byte array thành string
                var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
                var topic = e.ApplicationMessage.Topic;

                // Thử parse JSON, nếu không được thì hiển thị raw text
                try
                {
                    // Deserialize JSON thành ChatMessage object
                    var chatMessage = JsonSerializer.Deserialize<ChatMessage>(payload);

                    // Kiểm tra tin nhắn hợp lệ và không phải từ chính mình
                    if (chatMessage != null && chatMessage.From != clientId)
                    {
                        var timeStr = chatMessage.Timestamp.ToString("HH:mm:ss");
                        Console.WriteLine($"\n💬 [{timeStr}] {chatMessage.From} @{topic}: {chatMessage.Message}");

                        // Thêm vào lịch sử để user có thể xem lại
                        messageHistory.Add(chatMessage);
                    }
                    // Nếu tin nhắn từ chính mình thì không hiển thị (tránh duplicate)
                }
                catch
                {
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
                Console.Write("Choose option (1-6): ");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error processing message: {ex.Message}");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Hiển thị banner
        /// </summary>
        private static void ShowBanner()
        {
            Console.Clear();  // Xóa màn hình
            Console.ForegroundColor = ConsoleColor.Green;  // Đổi màu chữ

            Console.WriteLine(@"
╔══════════════════════════════════════════════════════════════╗
║  🎯 Tính năng:                                               ║
║     ✅ Connect to MQTT Broker                                ║
║     ✅ Subscribe/Unsubscribe Topics                          ║
║     ✅ Send & Receive Messages                               ║
║     ✅ Message History                                       ║
║     ✅ Real-time Chat                                        ║
║     ✅ JSON Message Format                                   ║
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

            Console.Write("Enter MQTT broker host (default: localhost): ");
            var host = Console.ReadLine()?.Trim();
            if (string.IsNullOrEmpty(host)) host = "localhost";  // Default value

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

            // Client ID phải unique trong toàn broker
            // Format: ChatClient_Username_TimeStamp để đảm bảo unique
            var clientId = $"ChatClient_{username}_{DateTime.Now:HHmmss}";

            return new UserInfo { Username = username, ClientId = clientId };
        }

        #endregion Feature Function
    }
}
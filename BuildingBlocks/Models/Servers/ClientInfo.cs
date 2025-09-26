using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BuildingBlocks.Models.Servers
{
    public class ClientInfo
    {
        public string ClientId { get; set; } = string.Empty;

        public DateTime ConnectedAt { get; set; }

        public DateTime LastActivity { get; set; }

        public int MessagesSent { get; set; } = 0;

        public string Endpoint { get; set; } = string.Empty;
    }
}

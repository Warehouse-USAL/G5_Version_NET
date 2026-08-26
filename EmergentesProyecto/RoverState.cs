using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EmergentesProyecto
{
    public class RoverState
    {
        public string Status { get; set; } = "disconnected";
        public DateTime LastSeen { get; set; }
        public bool Failed { get; set; }
    }
}

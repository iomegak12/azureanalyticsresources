using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;

namespace StormApplication1
{
    [Active(true)]
    class Program : TopologyDescriptor
    {
        static void Main(string[] args)
        {
        }

        public ITopologyBuilder GetTopologyBuilder()
        {
            TopologyBuilder topologyBuilder = new TopologyBuilder("StormApplication1" + DateTime.Now.ToString("yyyyMMddHHmmss"));
            topologyBuilder.SetSpout(
                "Spout",
                Spout.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"count"}}
                },
                1);
            topologyBuilder.SetBolt(
                "Bolt",
                Bolt.Get,
                new Dictionary<string, List<string>>(),
                1).shuffleGrouping("Spout");

            return topologyBuilder;
        }
    }
}


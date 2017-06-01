using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureHLSUploader;

namespace M3u8ParserSample
{
    class Program
    {
        static void Main(string[] args)
        {

            AzureHLSUploader.M3u8Parser parser;

            parser = new AzureHLSUploader.M3u8Parser("https://s3-us-west-2.amazonaws.com/odk-hls-seg/drama/love-affair-agent/love-affair-agent-e01/playlist_1080.m3u8");

            Task.Run(async () =>
            {
                AzureHLSUploader.Models.M3u8Entry entry;
                entry = await parser.ParseEntry();
                Console.WriteLine(entry.Playlists.Count + entry.Playlists.Sum(x => x.TsFiles.Count) + 1);
                Console.WriteLine(entry.Playlists.Count);

                List<string> lines = new List<string>();
                foreach(var playlist in entry.Playlists)
                {
                    lines.Add(playlist.Url);
                    foreach(var tsfile in playlist.TsFiles)
                    {
                        lines.Add(tsfile);
                    }
                }
                lines.Add(entry.Url);

                using (System.IO.StreamWriter file =  new System.IO.StreamWriter(@"love affair.txt"))
                {
                    foreach(var line in lines)
                    {
                        file.WriteLine(line);
                    }
                }

            }).GetAwaiter().GetResult();

            
        }
    }
}

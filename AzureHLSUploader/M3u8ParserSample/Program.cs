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

            parser = new AzureHLSUploader.M3u8Parser("https://s3-us-west-2.amazonaws.com/odk-hls-seg/v1/201705/religion/glory-church-of-jesus-christ/glory-church-of-jesus-christ-05312017-05312017/playlist_720.m3u8");

            

            Task.Run(async () =>
            {
                AzureHLSUploader.Models.M3u8Entry entry;
                entry = await parser.ParseEntry();
                Console.WriteLine(entry.Playlists.Count + entry.Playlists.Sum(x => x.TsFiles.Count) + 1);
                Console.WriteLine(entry.Playlists.Count);
            }).GetAwaiter().GetResult();

            
        }
    }
}

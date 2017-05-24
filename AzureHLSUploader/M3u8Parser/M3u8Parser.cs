using AzureHLSUploader.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace AzureHLSUploader
{
    public class M3u8Parser
    {
        public M3u8Parser(string url)
        {
            Entry = new M3u8Entry { Url = url };
            try
            {
                Uri uri = new Uri(url);
                var idx = uri.AbsolutePath.LastIndexOf('/');
                Entry.Path = uri.AbsolutePath.Substring(0, idx);
                Entry.Filename = uri.AbsolutePath.Substring(idx + 1);
                Entry.BaseUrl = uri.GetLeftPart(UriPartial.Authority);
                Entry.Playlists = new List<M3u8Playlist>();
            }
            catch(Exception ex)
            {
                throw new ArgumentException("Url address has error.", ex);
            }
        }

        public M3u8Entry Entry { get; set; }

        public async Task<M3u8Entry> ParseEntry()
        {
            using (HttpClient client = new HttpClient())
            {
                try
                {
                    if (Entry == null || string.IsNullOrEmpty(Entry.Url)) throw new ArgumentNullException("URL is not set.");

                    string m3u8 = await client.GetStringAsync(Entry.Url);
                    // playlist 분석
                    var lines = m3u8.Trim().Split('\n');
                    if (lines.Any())
                    {
                        if (!lines[0].ToUpper().Equals("#EXTM3U")) throw new ArgumentException("m3u8 file has error. not to start with #EXTM3U");
                        
                        for (var i = 1; i < lines.Length; i++)
                        {
                            var line = lines[i];
                            if (line.StartsWith("#"))
                            {
                                var directive = line.Substring(1);
                                var info = directive.Split(':');
                                var name = info[0];
                                var value = info[1];
                                var properties = ParseProperties(value);

                                switch(name.Trim().ToUpper())
                                {
                                    case "EXT-X-STREAM-INF":
                                        var filename = lines[i + 1];
                                        var metainfo = value.Split(',');
                                        string resolution = properties.Where(x => x.Key == "RESOLUTION").Select(x => x.Value).FirstOrDefault();
                                        string bandwidth = properties.Where(x => x.Key == "BANDWIDTH").Select(x => x.Value).FirstOrDefault();

                                        var playlist = new M3u8Playlist
                                        {
                                            Url = Entry.BaseUrl + Entry.Path + "/" + filename,
                                            Bandwidth = int.Parse(bandwidth),
                                            Resolution = resolution,
                                            Filename = filename,
                                        };

                                        await ParsePlaylist(playlist);
                                        Entry.Playlists.Add(playlist);

                                        break;
                                }
                            }
                        }
                    }
                    return Entry;

                }
                catch(Exception ex)
                {
                    throw new InvalidOperationException("M3U8 parse error", ex);
                }
            }
        }

        private Dictionary<string, string> ParseProperties(string data)
        {
            var result = new Dictionary<string, string>();
            var properties = data.Split(',');
            if (properties.Length <= 1) return result;

            foreach(var property in properties)
            {
                var p = property.Split('=');
                if (p.Length == 2) result.Add(p[0], p[1]);
            }
            return result;
        }

        private async Task ParsePlaylist(M3u8Playlist playlist)
        {
            using (HttpClient client = new HttpClient())
            {
                try
                {
                    if (Entry == null || string.IsNullOrEmpty(playlist.Url)) throw new ArgumentNullException("URL is not set.");
                    string m3u8 = await client.GetStringAsync(playlist.Url);
                    // ts 파일들 분석 
                    var lines = m3u8.Trim().Split('\n');
                    if (lines.Any())
                    {
                        if (!lines[0].ToUpper().Equals("#EXTM3U")) throw new ArgumentException("m3u8 file has error. not to start with #EXTM3U");

                        if (playlist.TsFiles == null) playlist.TsFiles = new List<string>();

                        for (var i = 1; i < lines.Length; i++)
                        {
                            var line = lines[i];
                            if (line.StartsWith("#"))
                            {
                                var directive = line.Substring(1);
                                var info = directive.Split(':');
                                
                                var name = info[0];
                                string value = "";
                                if (info.Length > 1) value = info[1];

                                switch (name.Trim().ToUpper())
                                {
                                    case "EXTINF":
                                        var filename = lines[i + 1];
                                        playlist.TsFiles.Add(filename);
                                        break;
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("M3U8 playlist parse error", ex);
                }
            }
        }
    }
}

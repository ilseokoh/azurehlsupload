using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureHLSUploader.Models
{
    public class M3u8Entry
    {
        public string Url { get; set; }
        public string BaseUrl { get; set; }
        public string Filename { get; set; }

        public string Path { get; set; }

        public List<M3u8Playlist> Playlists { get; set; }
    }
}

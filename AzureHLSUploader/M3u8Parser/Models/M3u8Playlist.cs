using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureHLSUploader.Models
{
    public class M3u8Playlist
    {
        public string Url { get; set; }

        public string Filename { get; set; }

        public int Bandwidth { get; set; }

        public string Resolution { get; set; }

        public List<string> TsFiles { get; set; }
    }
}

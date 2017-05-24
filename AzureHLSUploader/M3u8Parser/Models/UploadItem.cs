using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureHLSUploader.Models
{
    public class UploadItem
    {
        public string Url { get; set; }

        public bool NeedPreload { get; set; }

        public List<string> Items { get; set; }
    }
}
